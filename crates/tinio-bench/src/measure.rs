//! One named operation, measured in one process, reported on one line.
//!
//! A leg of the A/B protocol is this: the bench target is invoked once per
//! leg, it times the one operation it was asked for, and it prints an
//! `ab-result` line. [`run`] owns the whole shape of that leg —
//!
//! 1. the fixture is built, untimed (the operation's own constructor);
//! 2. `warmup` invocations run, untimed (the allocator window is not open yet);
//! 3. `iters` **timed samples** run, each of them `batch` invocations of the
//!    operation, each sample timed with its own `Instant` pair;
//! 4. the allocation window opened at step 3 is read, and the run's flush
//!    control is measured;
//! 5. the reading is printed — median, p90, minimum, allocations per
//!    invocation, and the flush control beside them.
//!
//! Two choices carry the protocol's weight:
//!
//! **`batch` exists because the timer is not free.** An `Instant::now()` pair
//! costs tens of nanoseconds on this host, so batching a sub-nanosecond
//! operation (a register op, an `Arc` clone) into samples of a thousand
//! invocations keeps that constant out of the number, while an operation that
//! already costs microseconds sets `batch = 1`. A batch above 1 trades the
//! timer's overhead for the resolution *inside* a sample: the reading is then
//! a mean over the batch, and the median is a median of means.
//!
//! **The distribution, not just the mean.** Every sample is kept, so the line
//! can carry the median and the p90 and the minimum beside it. On this host
//! the 90th percentile is what a mean hides: the flush regime's swings land in
//! the tail long before they move the median.

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{_util::bench, alloc};

/// A named operation, with the fixture it needs built on demand.
///
/// The name is what a leg selects (`--op <name>`) and what the reading line
/// carries back, so the orchestrator can see that both legs measured the
/// operation it asked for.
pub struct Op {
    /// The name a leg selects this operation by.
    name: &'static str,
    /// Invocations of the operation per timed sample.
    batch: u32,
    /// Builds the operation's body once per run, outside the timed loop.
    build: Box<dyn FnOnce() -> Box<dyn FnMut()>>,
}

impl Op {
    /// An operation named `name`, timed in samples of `batch` invocations.
    ///
    /// `build` runs once, before the warmup, and hands back the body: the
    /// fixture lives inside that closure's captures, so nothing the operation
    /// needs is constructed inside a timed sample. Use `batch = 1` for an
    /// operation that already costs microseconds; raise it for anything near
    /// the timer's own resolution.
    pub fn new(
        name: &'static str,
        batch: u32,
        build: impl FnOnce() -> Box<dyn FnMut()> + 'static,
    ) -> Self {
        Self {
            name,
            batch: batch.max(1),
            build: Box::new(build),
        }
    }

    /// The name a leg selects this operation by.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Invocations of the operation per timed sample.
    pub fn batch(&self) -> u32 {
        self.batch
    }
}

/// What one leg measured: the operation's own line.
#[derive(Debug, Clone)]
pub struct Reading {
    /// The operation measured.
    pub op: &'static str,
    /// Timed samples behind the numbers.
    pub iters: u32,
    /// Invocations per timed sample.
    pub batch: u32,
    /// Median sample, per invocation.
    pub p50_ns: f64,
    /// 90th-percentile sample, per invocation.
    pub p90_ns: f64,
    /// Fastest sample, per invocation.
    pub min_ns: f64,
    /// Allocation requests per invocation (exact: an integer count divided by
    /// the invocation total).
    pub allocs_per_op: f64,
    /// Bytes requested per invocation.
    pub bytes_per_op: f64,
    /// The run's flush control: one 1 KiB write plus `sync_all`, median of five
    /// (`tinio_util::bench::flush_control`).
    pub flush_ns: f64,
    /// The data root the run used — the regime the flush control describes.
    pub root: PathBuf,
}

impl Reading {
    /// The one line a leg prints, and the orchestrator parses.
    ///
    /// `root` is last because a path may contain spaces: a reader takes
    /// everything after `root=` to the end of the line.
    pub fn line(&self) -> String {
        format!(
            "ab-result op={} iters={} batch={} ops={} p50_ns={:.1} p90_ns={:.1} min_ns={:.1} \
             allocs_per_op={:.3} bytes_per_op={:.1} flush_ns={:.0} root={}",
            self.op,
            self.iters,
            self.batch,
            u64::from(self.iters) * u64::from(self.batch),
            self.p50_ns,
            self.p90_ns,
            self.min_ns,
            self.allocs_per_op,
            self.bytes_per_op,
            self.flush_ns,
            self.root.display(),
        )
    }
}

/// Measure `op`: `warmup` untimed invocations, then `iters` timed samples of
/// `op`'s batch, and the run's flush control.
///
/// The flush control is measured for every operation, including the ones that
/// never touch disk: it is the reading's statement about the host, and a
/// reading without one cannot be compared with anything.
pub fn run(op: Op, iters: u32, warmup: u32) -> Reading {
    let name = op.name;
    let batch = op.batch;
    let iters = iters.max(1);
    let root = bench::data_root();
    let mut body = (op.build)();
    for _ in 0..warmup {
        body();
    }

    let mut samples: Vec<Duration> = Vec::with_capacity(iters as usize);
    alloc::reset();
    for _ in 0..iters {
        let start = Instant::now();
        for _ in 0..batch {
            body();
        }
        samples.push(start.elapsed());
    }
    let usage = alloc::usage();

    // After the timed window: the control writes to disk, and its own
    // allocations are not the operation's.
    let flush = bench::flush_control(&root);

    samples.sort_unstable();
    let invocations = f64::from(iters) * f64::from(batch);
    Reading {
        op: name,
        iters,
        batch,
        p50_ns: per_op(percentile(&samples, 0.50), batch),
        p90_ns: per_op(percentile(&samples, 0.90), batch),
        min_ns: per_op(percentile(&samples, 0.0), batch),
        allocs_per_op: usage.allocations as f64 / invocations,
        bytes_per_op: usage.bytes as f64 / invocations,
        flush_ns: flush.as_nanos() as f64,
        root,
    }
}

/// A sample, in nanoseconds per invocation.
fn per_op(sample: Duration, batch: u32) -> f64 {
    sample.as_nanos() as f64 / f64::from(batch)
}

/// The percentile of a sorted sample set: `p` in `[0, 1]`, the sample at
/// `floor(len × p)`, clamped to the last one — so the p50 of an even count is
/// the upper of its two middle samples, and a batch's reading leans
/// conservative by that one sample.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (sorted.len() as f64 * p) as usize;
    sorted[rank.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::percentile;

    #[test]
    fn percentiles_take_the_sample_at_the_floored_rank() {
        let samples: Vec<Duration> = (1..=10).map(Duration::from_nanos).collect();
        assert_eq!(percentile(&samples, 0.00), Duration::from_nanos(1));
        assert_eq!(percentile(&samples, 0.50), Duration::from_nanos(6));
        assert_eq!(percentile(&samples, 0.90), Duration::from_nanos(10));
        assert_eq!(percentile(&samples, 1.00), Duration::from_nanos(10));
    }

    #[test]
    fn an_odd_sample_count_has_a_measured_median() {
        let samples: Vec<Duration> = (1..=5).map(Duration::from_nanos).collect();
        assert_eq!(percentile(&samples, 0.50), Duration::from_nanos(3));
    }

    #[test]
    fn an_empty_sample_set_is_zero() {
        assert_eq!(percentile(&[], 0.5), Duration::ZERO);
    }
}
