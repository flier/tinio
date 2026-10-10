//! The benches' data root, pinned, and the flush control every bench run
//! records — filed at close-out by the ops-engine design
//! (`docs/superpowers/specs/2026-10-03-ops-engine-design.md`, "A criterion
//! baseline is only comparable within its flush regime").
//!
//! The cells used to call `tempfile::tempdir()`, so a whole run's data path
//! followed the shell's `TMP`/`TEMP` — on the host that filed the rule,
//! `E:\Temp\tinio-tmp`, a file-backed virtual disk. A 1 KiB put performs two
//! durable flushes plus a directory sync, so the measured number tracks the
//! host's flush latency, which moves ~3× on a minutes timescale: a stored
//! baseline and a fresh run in different regimes produced a false "2.7×
//! regression" that was argued and re-measured for a day.
//!
//! Two rules come out of that, both encoded here:
//!
//! 1. every cell's root is `data_root` — an explicit directory on a fixed
//!    volume, overridable with `TINIO_BENCH_DATA_DIR`, **never** `TMP`;
//! 2. every run's log carries the `flush_control` measurement beside its
//!    numbers, so a stored baseline's flush regime is checkable after the
//!    fact.
//!
//! **This module is a library module behind the `bench` feature, not a
//! bench-local include**: `tinio-util` is a production dependency of several
//! crates, so bench-only code must not compile into a default build — the
//! gate is the `testing` feature's own pattern. The helper was born as the
//! file-scope include `crates/tinio-fs/benches/support/bench_root.rs`
//! (`docs/tests.md` §Rust: shared include = file-scope), which no other
//! crate's benches could reach; the operator's ruling moved it here so one
//! copy serves every bench target in the workspace. Its behaviour is
//! unchanged from that original — same root resolution order, same
//! measurement, same wording.

use std::{
    env, fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// The environment variable a runner overrides the data root with.
const ROOT_ENV: &str = "TINIO_BENCH_DATA_DIR";

/// The benches' data root: `$TINIO_BENCH_DATA_DIR` when set, otherwise
/// `<cargo target dir>/bench-data` — a path on the volume the build itself
/// lives on, so a bench run and its artifacts share one volume and a stored
/// baseline stays comparable with the run that reads it.
///
/// The fallback chain never touches `TMP`/`TEMP`: that is the rule this
/// module exists for.
pub fn data_root() -> PathBuf {
    if let Some(dir) = env::var_os(ROOT_ENV) {
        return PathBuf::from(dir);
    }
    if let Some(dir) = env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(dir).join("bench-data");
    }
    // `CARGO_MANIFEST_DIR` is `crates/tinio-util` (the crate this module
    // ships in), so the workspace's `target/` is two levels up (gitignored,
    // same volume as the repository).
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/bench-data")
}

/// A fresh, empty directory for one bench cell, under [`data_root`].
///
/// The previous run's files are removed first, so the cell starts from the
/// state `tempfile::tempdir()` used to hand it — the only thing that changes
/// between a pinned run and the tempdir era is *where* that state lives.
pub fn cell_dir(name: &str) -> PathBuf {
    // The name is a literal at every call site (a nested path is spelled with
    // `/`, which `Path::join` accepts): a wrong name is a typo in the bench,
    // not input to validate.
    let dir = data_root().join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).expect("the previous run's cell dir must clear");
    }
    fs::create_dir_all(&dir).expect("the cell dir must be creatable");
    dir
}

/// The flush-cost control: one 1 KiB write plus `sync_all`, the smallest
/// measurement that tracks the host's flush latency. The median of
/// `FLUSH_SAMPLES` samples is printed to the run log (stderr, so it lands
/// beside criterion's stdout when the runner redirects both), and returned.
///
/// A stored baseline's regime is only checkable afterwards if the run that
/// produced it recorded one of these; the baseline's log and this run's must
/// agree before two numbers are compared.
pub fn flush_control(root: &Path) -> Duration {
    let dir = root.join("flush-control");
    fs::create_dir_all(&dir).expect("the flush-control dir must be creatable");
    let path = dir.join("probe.bin");
    let payload = vec![b'z'; 1024];
    let mut samples = Vec::with_capacity(FLUSH_SAMPLES);
    for _ in 0..FLUSH_SAMPLES {
        let start = Instant::now();
        let mut file = File::create(&path).expect("the probe file must be creatable");
        file.write_all(&payload).expect("the probe write must land");
        file.sync_all().expect("the probe flush must complete");
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    eprintln!(
        "flush_control: 1 KiB write + sync_all median {median:?} over {FLUSH_SAMPLES} samples \
         (root: {})",
        root.display()
    );
    median
}

/// Samples behind [`flush_control`]'s median: odd, so the median is a
/// measured sample rather than an interpolation.
const FLUSH_SAMPLES: usize = 5;
