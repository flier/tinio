//! The cross-backend Prometheus families, declared once.
//!
//! A family two backends observe cannot live in either: the default registry
//! is process-global, so the second `register_*` is `AlreadyReg`. This module
//! owns the names the spec assigns to `tinio-util`; backends call
//! `record_*`, never `register_*`.
//!
//! The tier families are their own lazily registered group: only a process
//! that serves the tiered backend ever initializes them, and a second call to
//! any `record_tier_*` reuses the same families rather than registering a
//! duplicate.
//!
//! Labels are bounded by their call sites — `backend` (`fs`, `mem`, `pg`,
//! `tier`), `op`/`kind` (the failed write and its closed failure class), `pass`
//! (`fs_sweep`, `blob_sweep`, `pg_cleanup`), `section` (the failing stage),
//! `operation`/`reason` (the Object Lock refusal class), and the tier families'
//! `tier` (`warm`, `cold`) and `direction` (`promote`, `demote`). No path, key,
//! bucket or identifier may become a label.
//!
//! This is a metrics vocabulary, not a layer: no `tracing`, no `tokio`, and
//! only the model's [`RefusalReason`] from `tinio-core` — the one enum the
//! `reason` label is projected from, so its domain cannot drift.
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//!
//! use tinio_util::metrics::{
//!     Backend, FsSection, LockOperation, Pass, TierPlane, WriteFailureKind, WriteOp,
//!     record_corrupt_rows, record_fs_pass_failure, record_pass_duration,
//!     record_protected_deletion, record_read_only, record_tier_hit, record_write_failure,
//! };
//!
//! record_write_failure(Backend::Fs, WriteOp::Put, WriteFailureKind::Io);
//! record_pass_duration(Pass::FsSweep, Duration::from_millis(5));
//! record_fs_pass_failure(FsSection::Tmp);
//! record_read_only(Backend::Fs, false);
//! record_corrupt_rows(Backend::Fs, 1);
//! record_tier_hit(TierPlane::Warm);
//! record_protected_deletion(
//!     LockOperation::Lifecycle,
//!     tinio_core::object_lock::RefusalReason::Governance,
//! );
//! assert!(!prometheus::default_registry().gather().is_empty());
//! ```

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use prometheus::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec,
};
use tinio_core::object_lock::RefusalReason;

/// Pass-duration buckets, in seconds: one pass ranges from a sub-second
/// scan to the hour-scale sweep's walk.
pub const PASS_DURATION_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 3600.0,
];

/// Tier move and flush buckets, in seconds: a move copies bytes between two
/// planes, so its sub-second range matters as much as the hour-scale flush.
pub const TIER_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 3600.0,
];

/// The registered families.
static FAMILIES: LazyLock<Families> = LazyLock::new(|| Families {
    write_failures: register_int_counter_vec!(
        "tinio_storage_write_failures_total",
        "Storage writes refused or failed, by backend, operation and failure kind",
        &["backend", "op", "kind"]
    )
    .registered("tinio_storage_write_failures_total"),
    pass_duration: register_histogram_vec!(
        "tinio_storage_pass_duration_seconds",
        "Background storage pass durations, by pass",
        &["pass"],
        PASS_DURATION_BUCKETS.to_vec()
    )
    .registered("tinio_storage_pass_duration_seconds"),
    pass_failures: register_int_counter_vec!(
        "tinio_storage_pass_failures_total",
        "Background storage pass failures, by pass and failing section",
        &["pass", "section"]
    )
    .registered("tinio_storage_pass_failures_total"),
    read_only: register_int_gauge_vec!(
        "tinio_storage_read_only",
        "Whether a backend serves read-only (1) or read-write (0), by backend",
        &["backend"]
    )
    .registered("tinio_storage_read_only"),
    corrupt_rows: register_int_counter_vec!(
        "tinio_storage_corrupt_rows_total",
        "Stored rows that failed domain validation, by backend",
        &["backend"]
    )
    .registered("tinio_storage_corrupt_rows_total"),
    protected_deletions: register_int_counter_vec!(
        "tinio_object_lock_protected_deletions_total",
        "Permanent-delete refusals by a stored Object Lock, counted per refusal: \
         the lifecycle pass counts refusals per pass, not distinct versions, by \
         operation and reason",
        &["operation", "reason"]
    )
    .registered("tinio_object_lock_protected_deletions_total"),
});

/// The tier families, in their own `LazyLock`: a process that never serves
/// the tiered backend registers none of them, and every later
/// `record_tier_*` reuses the one registration.
static TIER_FAMILIES: LazyLock<TierFamilies> = LazyLock::new(|| TierFamilies {
    hits: register_int_counter_vec!(
        "tinio_tier_hits_total",
        "Object reads a tier served, by tier",
        &["tier"]
    )
    .registered("tinio_tier_hits_total"),
    misses: register_int_counter_vec!(
        "tinio_tier_misses_total",
        "Object reads a tier could not serve, by tier",
        &["tier"]
    )
    .registered("tinio_tier_misses_total"),
    bytes: register_int_counter_vec!(
        "tinio_tier_bytes_total",
        "Bytes served by a tier, by tier (a full GET's size, a ranged GET's inclusive \
         extent; HEAD contributes nothing)",
        &["tier"]
    )
    .registered("tinio_tier_bytes_total"),
    moves: register_int_counter_vec!(
        "tinio_tier_moves_total",
        "Objects moved between planes, by direction (a client copy, rename or copy_part that \
         promotes a cold source books a promote, as the mover's own moves do)",
        &["direction"]
    )
    .registered("tinio_tier_moves_total"),
    move_duration: register_histogram_vec!(
        "tinio_tier_move_duration_seconds",
        "Object move durations, by direction",
        &["direction"],
        TIER_DURATION_BUCKETS.to_vec()
    )
    .registered("tinio_tier_move_duration_seconds"),
    flush_duration: register_histogram!(
        "tinio_tier_flush_duration_seconds",
        "Flush durations, one sample per drain that published an entry",
        TIER_DURATION_BUCKETS.to_vec()
    )
    .registered("tinio_tier_flush_duration_seconds"),
    orphans: register_int_gauge!(
        "tinio_tier_orphans",
        "Cold copies a verify or garbage-collection pass reported as orphans"
    )
    .registered("tinio_tier_orphans"),
    quarantine: register_int_gauge!(
        "tinio_tier_quarantine",
        "Quarantined batch writes waiting for an operator"
    )
    .registered("tinio_tier_quarantine"),
    cache_evictions: register_int_counter!(
        "tinio_tier_cache_evictions_total",
        "Cached objects evicted before they were served"
    )
    .registered("tinio_tier_cache_evictions_total"),
});

/// Registration helper: a duplicate or malformed family name is a source-level
/// collision, so the process refuses it at start-up instead of carrying a
/// wrong label set.
pub trait RegisteredExt<T> {
    /// Unwrap a registration, panicking with the family name on a collision.
    fn registered(self, name: &'static str) -> T;
}

impl<T> RegisteredExt<T> for Result<T, prometheus::Error> {
    #[track_caller]
    fn registered(self, name: &'static str) -> T {
        #[expect(
            clippy::panic,
            reason = "argued: a duplicate family name is a source-level collision, not a runtime condition; the process cannot serve a wrong label set"
        )]
        match self {
            Ok(value) => value,
            Err(err) => panic!("register {name}: {err}"),
        }
    }
}

/// The six families, held together so one `LazyLock` runs the six
/// registrations.
struct Families {
    write_failures: IntCounterVec,
    pass_duration: HistogramVec,
    pass_failures: IntCounterVec,
    read_only: IntGaugeVec,
    corrupt_rows: IntCounterVec,
    protected_deletions: IntCounterVec,
}

/// The tier families, held together so one `LazyLock` runs the nine
/// registrations exactly once.
struct TierFamilies {
    hits: IntCounterVec,
    misses: IntCounterVec,
    bytes: IntCounterVec,
    moves: IntCounterVec,
    move_duration: HistogramVec,
    flush_duration: Histogram,
    orphans: IntGauge,
    quarantine: IntGauge,
    cache_evictions: IntCounter,
}

/// A status code as a metric label: the common codes are static, the rest
/// format into the caller's `itoa::Buffer`.
#[macro_export]
macro_rules! status_label {
    ($status:expr, $buffer:ident) => {
        match $status {
            200 => "200",
            201 => "201",
            204 => "204",
            206 => "206",
            301 => "301",
            304 => "304",
            400 => "400",
            401 => "401",
            403 => "403",
            404 => "404",
            405 => "405",
            409 => "409",
            412 => "412",
            413 => "413",
            416 => "416",
            500 => "500",
            501 => "501",
            503 => "503",
            other => $buffer.format(other),
        }
    };
}

/// The backend a storage family observes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// The filesystem backend.
    Fs,
    /// The in-memory backend.
    Mem,
    /// The PostgreSQL backend.
    Pg,
    /// The tiered backend: the filesystem warm plane plus the PostgreSQL
    /// cold plane.
    Tier,
}

impl Backend {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fs => "fs",
            Self::Mem => "mem",
            Self::Pg => "pg",
            Self::Tier => "tier",
        }
    }
}

/// The operation a failed write belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// A whole object write.
    Put,
    /// A multipart part write.
    Part,
    /// An object or bucket delete.
    Delete,
    /// Startup or background cleanup.
    Cleanup,
}

impl WriteOp {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Part => "part",
            Self::Delete => "delete",
            Self::Cleanup => "cleanup",
        }
    }
}

/// The closed failure class of a refused or failed write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFailureKind {
    /// The backend's capacity or size cap refused it.
    Cap,
    /// The storage layer failed.
    Io,
    /// A read-only backend refused a mutation.
    ReadOnly,
}

impl WriteFailureKind {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cap => "cap",
            Self::Io => "io",
            Self::ReadOnly => "read_only",
        }
    }
}

/// Record one refused or failed storage write.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{Backend, WriteFailureKind, WriteOp, record_write_failure};
///
/// record_write_failure(Backend::Mem, WriteOp::Put, WriteFailureKind::Cap);
/// ```
pub fn record_write_failure(backend: Backend, op: WriteOp, kind: WriteFailureKind) {
    FAMILIES
        .write_failures
        .with_label_values(&[backend.as_str(), op.as_str(), kind.as_str()])
        .inc();
}

/// The background pass a duration or failure belongs to — the closed `pass` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// The filesystem sweeper.
    FsSweep,
    /// The block-store sweep.
    BlobSweep,
    /// The PostgreSQL cleanup pass.
    PgCleanup,
}

impl Pass {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FsSweep => "fs_sweep",
            Self::BlobSweep => "blob_sweep",
            Self::PgCleanup => "pg_cleanup",
        }
    }
}

/// The failing stage of the filesystem sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsSection {
    /// Lifecycle rules.
    Rules,
    /// Versioned rows.
    Versioned,
    /// Orphaned block rows.
    OrphanBlobs,
    /// Staged temp files.
    Tmp,
    /// Multipart uploads.
    Multipart,
}

impl FsSection {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Versioned => "versioned",
            Self::OrphanBlobs => "orphan_blobs",
            Self::Tmp => "tmp",
            Self::Multipart => "multipart",
        }
    }
}

/// The failing stage of the block sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobSection {
    /// The block walk.
    Walk,
    /// The removal phase.
    Remove,
}

impl BlobSection {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Walk => "walk",
            Self::Remove => "remove",
        }
    }
}

/// The failing stage of the PostgreSQL cleanup pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgSection {
    /// Lifecycle rules.
    Rules,
    /// The uploads phase.
    Uploads,
    /// The chunks phase.
    Chunks,
    /// The parts phase.
    Parts,
    /// The versions phase.
    Versions,
    /// The blocks phase.
    Blocks,
}

impl PgSection {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Uploads => "uploads",
            Self::Chunks => "chunks",
            Self::Parts => "parts",
            Self::Versions => "versions",
            Self::Blocks => "blocks",
        }
    }
}

/// Record one completed background pass's duration.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_util::metrics::{Pass, record_pass_duration};
///
/// record_pass_duration(Pass::BlobSweep, Duration::from_millis(40));
/// ```
pub fn record_pass_duration(pass: Pass, duration: Duration) {
    FAMILIES
        .pass_duration
        .with_label_values(&[pass.as_str()])
        .observe(duration.as_secs_f64());
}

/// A guard that records one background pass's duration on drop, so every exit path — including
/// an early return — books exactly one sample.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{Pass, PassTimer};
///
/// let _pass = PassTimer::new(Pass::BlobSweep);
/// // ... the pass ...
/// ```
#[must_use = "the sample is recorded on drop"]
pub struct PassTimer {
    pass: Pass,
    started: Instant,
}

impl PassTimer {
    /// Start timing one pass.
    pub fn new(pass: Pass) -> Self {
        Self {
            pass,
            started: Instant::now(),
        }
    }
}

impl Drop for PassTimer {
    fn drop(&mut self) {
        record_pass_duration(self.pass, self.started.elapsed());
    }
}

/// Record one failed section of the filesystem sweep.
pub fn record_fs_pass_failure(section: FsSection) {
    record_pass_failure(Pass::FsSweep, section.as_str());
}

/// Record one failed section of the block sweep.
pub fn record_blob_pass_failure(section: BlobSection) {
    record_pass_failure(Pass::BlobSweep, section.as_str());
}

/// Record one failed section of the PostgreSQL cleanup pass.
pub fn record_pg_pass_failure(section: PgSection) {
    record_pass_failure(Pass::PgCleanup, section.as_str());
}

fn record_pass_failure(pass: Pass, section: &'static str) {
    FAMILIES
        .pass_failures
        .with_label_values(&[pass.as_str(), section])
        .inc();
}

/// Record whether a backend serves read-only: `1` when it does, `0` when it
/// does not.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{Backend, record_read_only};
///
/// record_read_only(Backend::Fs, true);
/// ```
pub fn record_read_only(backend: Backend, read_only: bool) {
    FAMILIES
        .read_only
        .with_label_values(&[backend.as_str()])
        .set(if read_only { 1 } else { 0 });
}

/// Record `count` stored rows that failed domain validation.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{Backend, record_corrupt_rows};
///
/// record_corrupt_rows(Backend::Pg, 1);
/// ```
pub fn record_corrupt_rows(backend: Backend, count: u64) {
    FAMILIES
        .corrupt_rows
        .with_label_values(&[backend.as_str()])
        .inc_by(count);
}

/// The physical tier a tier family observes: the filesystem warm plane and
/// the PostgreSQL cold plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierPlane {
    /// The filesystem warm plane.
    Warm,
    /// The PostgreSQL cold plane.
    Cold,
}

impl TierPlane {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warm => "warm",
            Self::Cold => "cold",
        }
    }
}

/// The direction of one object move between the tier planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveDirection {
    /// A cold object moved back to the warm plane — the mover's promotion,
    /// and a client copy, rename or `copy_part` that promotes a cold source.
    Promote,
    /// A warm object moved to the cold plane.
    Demote,
}

impl MoveDirection {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Promote => "promote",
            Self::Demote => "demote",
        }
    }
}

/// Record one read a tier served.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{TierPlane, record_tier_hit};
///
/// record_tier_hit(TierPlane::Warm);
/// ```
pub fn record_tier_hit(tier: TierPlane) {
    TIER_FAMILIES.hits.with_label_values(&[tier.as_str()]).inc();
}

/// Record one read a tier could not serve — the object was not there, so the
/// read went on to the next plane.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{TierPlane, record_tier_miss};
///
/// record_tier_miss(TierPlane::Cold);
/// ```
pub fn record_tier_miss(tier: TierPlane) {
    TIER_FAMILIES
        .misses
        .with_label_values(&[tier.as_str()])
        .inc();
}

/// Record the bytes one served object read handed the caller.
///
/// A full GET books the object's size, a ranged GET books the inclusive
/// extent `end - start + 1`, and a HEAD books nothing because it serves no
/// body — its caller must not call this.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{TierPlane, record_tier_bytes};
///
/// record_tier_bytes(TierPlane::Cold, 4096);
/// ```
pub fn record_tier_bytes(tier: TierPlane, bytes: u64) {
    TIER_FAMILIES
        .bytes
        .with_label_values(&[tier.as_str()])
        .inc_by(bytes);
}

/// Record one object move between the planes in `direction`.
///
/// A client-side copy or rename of a cold source books a
/// [`MoveDirection::Promote`] too: the bytes changed plane, whichever path
/// drove them. No label separates the paths — the direction is the axis.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{MoveDirection, record_tier_move};
///
/// record_tier_move(MoveDirection::Promote);
/// ```
pub fn record_tier_move(direction: MoveDirection) {
    TIER_FAMILIES
        .moves
        .with_label_values(&[direction.as_str()])
        .inc();
}

/// Record one completed object move's duration, in `direction`.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_util::metrics::{MoveDirection, record_tier_move_duration};
///
/// record_tier_move_duration(MoveDirection::Demote, Duration::from_millis(40));
/// ```
pub fn record_tier_move_duration(direction: MoveDirection, duration: Duration) {
    TIER_FAMILIES
        .move_duration
        .with_label_values(&[direction.as_str()])
        .observe(duration.as_secs_f64());
}

/// Record one completed batch flush's duration.
///
/// A sample is booked only for a drain that published at least one entry: an
/// empty drain is not a flush, and a drain whose every entry was quarantined
/// published nothing. A single-key `flush_key` that publishes counts.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_util::metrics::record_tier_flush_duration;
///
/// record_tier_flush_duration(Duration::from_millis(15));
/// ```
pub fn record_tier_flush_duration(duration: Duration) {
    TIER_FAMILIES.flush_duration.observe(duration.as_secs_f64());
}

/// Set the number of orphaned cold copies the last verify or garbage pass
/// reported.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::record_tier_orphans;
///
/// record_tier_orphans(3);
/// ```
pub fn record_tier_orphans(count: u64) {
    TIER_FAMILIES.orphans.set(count as i64);
}

/// Set the number of quarantined batch writes waiting for an operator.
///
/// A batch write whose flush fails every retry is persisted under the layer's
/// state directory and dropped from the buffer; the gauge reports how many
/// such records are waiting, and it returns to zero when they are cleared.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::record_tier_quarantine;
///
/// record_tier_quarantine(2);
/// ```
pub fn record_tier_quarantine(count: u64) {
    TIER_FAMILIES.quarantine.set(count as i64);
}

/// Record `count` cached objects evicted before they were served.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::record_tier_cache_eviction;
///
/// record_tier_cache_eviction(2);
/// ```
pub fn record_tier_cache_eviction(count: u64) {
    TIER_FAMILIES.cache_evictions.inc_by(count);
}

/// The operation whose permanent delete a stored Object Lock refused — the
/// closed `operation` label of
/// `tinio_object_lock_protected_deletions_total` (OL25).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOperation {
    /// A single `DeleteObject?versionId`.
    DeleteObject,
    /// A `DeleteObjects` batch entry.
    DeleteObjects,
    /// The lifecycle pass's noncurrent expiration. This label's counter is the
    /// number of refusals the passes made, so a version the rule re-scans each
    /// pass is counted once per pass.
    Lifecycle,
    /// A backend sweeper's retention pass.
    Sweeper,
}

impl LockOperation {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteObject => "delete_object",
            Self::DeleteObjects => "delete_objects",
            Self::Lifecycle => "lifecycle",
            Self::Sweeper => "sweeper",
        }
    }
}

/// The metric label for a model refusal reason (OL11, OL25).
///
/// The model's `Display` is the operator-facing text (`"legal hold"`, with a
/// space), never a wire or label value, so the label is mapped explicitly.
/// There is deliberately no mirror enum: a second type and a hand-written
/// conversion would be the same fact twice, and the model's domain is already
/// exactly the label set.
fn reason_label(reason: RefusalReason) -> &'static str {
    match reason {
        RefusalReason::LegalHold => "legal_hold",
        RefusalReason::Compliance => "compliance",
        RefusalReason::Governance => "governance",
    }
}

/// Record one permanent delete a stored Object Lock refused.
///
/// This is the one entry point for the family: the S3 delete sites, the
/// lifecycle pass and the backends' sweepers all call it, passing the
/// [`RefusalReason`] their `Locked` error carries straight through — so the
/// family's two bounded labels have one producer and no path, key, bucket or
/// version can become a series.
///
/// The counter is **per refusal, not per version**: a lifecycle pass that
/// re-scans a still-protected version on every pass increments once per pass.
/// Read it as "how often protection stopped a delete", not as a distinct-version
/// gauge.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::RefusalReason;
/// use tinio_util::metrics::{LockOperation, record_protected_deletion};
///
/// record_protected_deletion(LockOperation::DeleteObject, RefusalReason::LegalHold);
/// ```
pub fn record_protected_deletion(operation: LockOperation, reason: RefusalReason) {
    FAMILIES
        .protected_deletions
        .with_label_values(&[operation.as_str(), reason_label(reason)])
        .inc();
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, TextEncoder, default_registry};

    use super::*;
    #[cfg(feature = "testing")]
    use crate::testing::metrics::protected_deletions;

    fn exposition() -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&default_registry().gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn the_six_families_render_their_recorded_values() {
        record_write_failure(Backend::Fs, WriteOp::Put, WriteFailureKind::Io);
        record_pass_duration(Pass::BlobSweep, Duration::from_millis(250));
        record_blob_pass_failure(BlobSection::Walk);
        record_read_only(Backend::Fs, true);
        record_corrupt_rows(Backend::Fs, 2);
        record_protected_deletion(LockOperation::Lifecycle, RefusalReason::Compliance);

        let text = exposition();
        assert!(
            text.contains(r#"tinio_storage_write_failures_total{backend="fs",kind="io",op="put"}"#),
            "the write-failures counter carries its labels and value: {text}"
        );
        assert!(
            text.contains(r#"tinio_storage_pass_duration_seconds_count{pass="blob_sweep"}"#),
            "the pass-duration histogram records one sample: {text}"
        );
        assert!(
            text.contains(r#"tinio_storage_pass_duration_seconds_sum{pass="blob_sweep"}"#),
            "the pass-duration histogram records its seconds: {text}"
        );
        assert!(
            text.contains(r#"tinio_storage_pass_failures_total{pass="blob_sweep",section="walk"}"#),
            "the pass-failures counter carries its section: {text}"
        );
        assert!(
            text.contains(r#"tinio_storage_read_only{backend="fs"}"#),
            "the read-only gauge reports its backend: {text}"
        );
        assert!(
            text.contains(r#"tinio_storage_corrupt_rows_total{backend="fs"}"#),
            "the corrupt-rows counter accumulates: {text}"
        );
        assert!(
            text.contains(
                r#"tinio_object_lock_protected_deletions_total{operation="lifecycle",reason="compliance"}"#
            ),
            "the protected-deletions counter carries its two labels: {text}"
        );
    }

    #[test]
    fn the_tier_families_render_their_recorded_values() {
        record_tier_hit(TierPlane::Cold);
        record_tier_miss(TierPlane::Warm);
        record_tier_bytes(TierPlane::Cold, 4096);
        record_tier_move(MoveDirection::Promote);
        record_tier_move_duration(MoveDirection::Demote, Duration::from_millis(7));
        record_tier_flush_duration(Duration::from_millis(3));
        record_tier_orphans(2);
        record_tier_quarantine(4);
        record_tier_cache_eviction(3);

        let text = exposition();
        assert!(
            text.contains(r#"tinio_tier_hits_total{tier="cold"}"#),
            "the tier hits counter carries its tier: {text}"
        );
        assert!(
            text.contains(r#"tinio_tier_misses_total{tier="warm"}"#),
            "the tier misses counter carries its tier: {text}"
        );
        assert!(
            text.contains(r#"tinio_tier_bytes_total{tier="cold"} 4096"#),
            "the tier bytes counter accumulates its served bytes: {text}"
        );
        assert!(
            text.contains(r#"tinio_tier_moves_total{direction="promote"}"#),
            "the move counter carries its direction: {text}"
        );
        assert!(
            text.contains(r#"tinio_tier_move_duration_seconds_sum{direction="demote"}"#),
            "the move histogram records its seconds: {text}"
        );
        assert!(
            text.contains("tinio_tier_flush_duration_seconds_sum"),
            "the flush histogram records its seconds: {text}"
        );
        assert!(
            text.contains("tinio_tier_orphans 2"),
            "the orphan gauge reports the count: {text}"
        );
        assert!(
            text.contains("tinio_tier_quarantine 4"),
            "the quarantine gauge reports the waiting count: {text}"
        );
        assert!(
            text.contains("tinio_tier_cache_evictions_total 3"),
            "the eviction counter accumulates: {text}"
        );
    }

    #[test]
    fn the_protected_deletion_family_registers_once_with_both_label_domains() {
        for operation in [
            LockOperation::DeleteObject,
            LockOperation::DeleteObjects,
            LockOperation::Lifecycle,
            LockOperation::Sweeper,
        ] {
            for reason in [
                RefusalReason::LegalHold,
                RefusalReason::Compliance,
                RefusalReason::Governance,
            ] {
                record_protected_deletion(operation, reason);
            }
        }

        let gathered = default_registry().gather();
        assert_eq!(
            gathered
                .iter()
                .filter(|family| family.name() == "tinio_object_lock_protected_deletions_total")
                .count(),
            1,
            "the family is registered exactly once"
        );
        let text = exposition();
        for operation in ["delete_object", "delete_objects", "lifecycle", "sweeper"] {
            for reason in ["legal_hold", "compliance", "governance"] {
                let series = format!(
                    r#"tinio_object_lock_protected_deletions_total{{operation="{operation}",reason="{reason}"}}"#
                );
                assert!(
                    text.contains(&series),
                    "the family exposes {operation}/{reason}: {text}"
                );
                // The shared delta fixture (behind `testing`) reads the same
                // series the exposition renders; every pair was recorded
                // above, so the value can only have grown.
                #[cfg(feature = "testing")]
                assert!(
                    protected_deletions(operation, reason) >= 1.0,
                    "the shared fixture reads {operation}/{reason}"
                );
            }
        }
    }
}
