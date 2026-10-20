//! The five cross-backend Prometheus families, declared once.
//!
//! A family two backends observe cannot live in either: the default registry
//! is process-global, so the second `register_*` is `AlreadyReg`. This module
//! owns the names the spec assigns to `tinio-util`; backends call
//! `record_*`, never `register_*`.
//!
//! Labels are bounded by their call sites — `backend` (`fs`, `mem`, `pg`),
//! `op`/`kind` (the failed write and its closed failure class), `pass`
//! (`fs_sweep`, `blob_sweep`, `pg_cleanup`) and `section` (the failing
//! stage). No path, key, bucket or identifier may become a label.
//!
//! This is a metrics vocabulary, not a layer: no `tracing`, no `tinio-core`
//! type, no `tokio`.
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//!
//! use tinio_util::metrics::{
//!     Backend, Pass, Section, WriteFailureKind, WriteOp, record_corrupt_rows,
//!     record_pass_duration, record_pass_failure, record_read_only, record_write_failure,
//! };
//!
//! record_write_failure(Backend::Fs, WriteOp::Put, WriteFailureKind::Io);
//! record_pass_duration(Pass::FsSweep, Duration::from_millis(5));
//! record_pass_failure(Pass::FsSweep, Section::Tmp);
//! record_read_only(Backend::Fs, false);
//! record_corrupt_rows(Backend::Fs, 1);
//! assert!(!prometheus::default_registry().gather().is_empty());
//! ```

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, register_histogram_vec, register_int_counter_vec,
    register_int_gauge_vec,
};

/// Pass-duration buckets, in seconds: one pass ranges from a sub-second
/// scan to the hour-scale sweep's walk.
pub const PASS_DURATION_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 3600.0,
];

/// The registered families.
#[expect(
    clippy::expect_used,
    reason = "argued: every error a registration can answer is about this family's own name — the process-global default registry refuses a name it already holds or holds with a different label set or help. Each name has one registration site in the repo (this `LazyLock`, whose closure runs once per process) against the spec's family list; a collision is a source-level mistake, not a runtime condition, and `LazyLock::new` has no `Result` to return it through. The module's test reads all five names back out of the gathered exposition, so a registration that did not land fails a test."
)]
static FAMILIES: LazyLock<Families> = LazyLock::new(|| Families {
    write_failures: register_int_counter_vec!(
        "tinio_storage_write_failures_total",
        "Storage writes refused or failed, by backend, operation and failure kind",
        &["backend", "op", "kind"]
    )
    .expect("register tinio_storage_write_failures_total"),
    pass_duration: register_histogram_vec!(
        "tinio_storage_pass_duration_seconds",
        "Background storage pass durations, by pass",
        &["pass"],
        PASS_DURATION_BUCKETS.to_vec()
    )
    .expect("register tinio_storage_pass_duration_seconds"),
    pass_failures: register_int_counter_vec!(
        "tinio_storage_pass_failures_total",
        "Background storage pass failures, by pass and failing section",
        &["pass", "section"]
    )
    .expect("register tinio_storage_pass_failures_total"),
    read_only: register_int_gauge_vec!(
        "tinio_storage_read_only",
        "Whether a backend serves read-only (1) or read-write (0), by backend",
        &["backend"]
    )
    .expect("register tinio_storage_read_only"),
    corrupt_rows: register_int_counter_vec!(
        "tinio_storage_corrupt_rows_total",
        "Stored rows that failed domain validation, by backend",
        &["backend"]
    )
    .expect("register tinio_storage_corrupt_rows_total"),
});

/// The five families, held together so one `LazyLock` runs the five
/// registrations.
struct Families {
    write_failures: IntCounterVec,
    pass_duration: HistogramVec,
    pass_failures: IntCounterVec,
    read_only: IntGaugeVec,
    corrupt_rows: IntCounterVec,
}

/// A status code as a metric label: the common codes are static, the rest
/// format into the caller's [`itoa::Buffer`].
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
}

impl Backend {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fs => "fs",
            Self::Mem => "mem",
            Self::Pg => "pg",
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

/// The failing stage of a background pass — the closed `section` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
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
    /// The block walk.
    Walk,
    /// The removal phase.
    Remove,
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

impl Section {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Versioned => "versioned",
            Self::OrphanBlobs => "orphan_blobs",
            Self::Tmp => "tmp",
            Self::Multipart => "multipart",
            Self::Walk => "walk",
            Self::Remove => "remove",
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

/// Record one failed section of a background pass.
///
/// # Examples
///
/// ```rust
/// use tinio_util::metrics::{Pass, Section, record_pass_failure};
///
/// record_pass_failure(Pass::PgCleanup, Section::Blocks);
/// ```
pub fn record_pass_failure(pass: Pass, section: Section) {
    FAMILIES
        .pass_failures
        .with_label_values(&[pass.as_str(), section.as_str()])
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
    fn the_five_families_render_their_recorded_values() {
        record_write_failure(Backend::Fs, WriteOp::Put, WriteFailureKind::Io);
        record_pass_duration(Pass::BlobSweep, Duration::from_millis(250));
        record_pass_failure(Pass::BlobSweep, Section::Walk);
        record_read_only(Backend::Fs, true);
        record_corrupt_rows(Backend::Fs, 2);

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
    }
}
