//! Criterion bench: CSV full scan vs filtered scan (select-object-content
//! plan Task 15).
//!
//! Fixture: a 100k-row, 4-column headerless CSV (default `_1.._4` names)
//! generated once in setup and held in memory — in-memory only, no
//! tempfile (the crate's manifest note). Column `_4` alternates `1`/`-1`,
//! so `s._4 > 0` keeps exactly half the rows: the two groups compare the
//! same scan volume with and without a per-row predicate.
//!
//! Each iteration drives `sql::parse` (parsed once, per group) +
//! `events::select_iter` over a **fresh** `Cursor` clone of the CSV —
//! `iter_batched` with `BatchSize::SmallInput` (the setup clone is outside
//! the timed region, so it measures the scan, not the copy). The stream is
//! drained to `Stats` + `End`, event payloads black-boxed against
//! dead-code elimination, and the `Stats` `bytes_scanned` is returned as
//! the routine's observable. Throughput is `Throughput::Bytes(csv.len())`
//! — the whole object is scanned in both groups (the filter applies per
//! row after parse, so filtered scan's IO volume is identical); criterion
//! reports it as MiB/s. `Cont`/`Stats`/`End` events use the default
//! `SelectConfig` (the server's `ContPolicy`).

use std::hint::black_box;
use std::io::{Cursor, Read};
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use tinio_select::events::{SelectConfig, SelectEvent, select_iter};
use tinio_select::sql::{QueryPlan, parse};

/// Fixture rows (plan Task 15: 100k-row CSV).
const ROWS: usize = 100_000;
/// Per-iteration cap on samples/latency; the `--quick` run overrides both.
const WARM_UP: u64 = 1;
const MEASURE: u64 = 5;

/// One headerless 4-column CSV: `i,alpha-i,beta-i,±1`. `_4` alternates so
/// the filtered group's predicate selects exactly half the rows.
fn generate_csv() -> Vec<u8> {
    let mut csv = String::with_capacity(ROWS * 36);
    for i in 0..ROWS {
        csv.push_str(&format!(
            "{i},alpha-{i:08},beta-{i:08},{}\n",
            if i % 2 == 0 { 1 } else { -1 }
        ));
    }
    csv.into_bytes()
}

/// One scan: stream to `Stats` + `End`, black-boxing every event against
/// DCE, and return the reported `bytes_scanned`.
fn drain(plan: QueryPlan, config: SelectConfig, input: Box<dyn Read + Send>) -> u64 {
    let mut scanned = 0u64;
    for ev in select_iter(plan, config, input) {
        let ev = black_box(ev.unwrap_or_else(|e| panic!("select stream error: {e}")));
        if let SelectEvent::Stats { bytes_scanned, .. } = ev {
            scanned = bytes_scanned;
        }
    }
    scanned
}

/// One criterion group: the fixture, the parsed plan, `iter_batched` runs.
fn scan_group(c: &mut Criterion, group_name: &str, fname: &str, sql: &str) {
    let csv = generate_csv();
    let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let config = SelectConfig::default();
    let mut group = c.benchmark_group(group_name);
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(csv.len() as u64));
    group.bench_function(fname, |b| {
        b.iter_batched(
            || Box::new(Cursor::new(csv.clone())) as Box<dyn Read + Send>,
            |input| drain(plan.clone(), config.clone(), input),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// No predicate: everything through the pipeline.
fn full_scan(c: &mut Criterion) {
    scan_group(c, "full_scan", "select_star", "SELECT * FROM S3Object");
}

/// Half the rows pass `_4 > 0` — the predicate cost over the same scan.
fn filtered_scan(c: &mut Criterion) {
    scan_group(
        c,
        "filtered_scan",
        "where_positive",
        "SELECT * FROM S3Object s WHERE s._4 > 0",
    );
}

criterion_group!(benches, full_scan, filtered_scan);
criterion_main!(benches);
