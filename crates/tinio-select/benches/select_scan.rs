//! Criterion bench: CSV full scan vs filtered scan (select-object-content
//! plan Task 15) plus the JSON DOCUMENT scan (json-streaming plan Task 11').
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
//!
//! `json_document_scan` drives the streaming `json::Reader` façade (Task 8)
//! over an 897_751-byte array fixture. That fixture is deliberately under the
//! old 1 MB whole-document cap, so the streaming rewrite is compared against
//! the **pre-refactor** criterion baseline (which the old façade measured)
//! with the same command.
//!
//! `json_lines_scan` drives the same façade over the same 10 000 elements
//! rendered as newline-separated LINES records. The LINES path builds a fresh
//! `JsonStreamReader` per record — two heap allocations and a ~1 KiB buffer
//! zeroing each time — so the pair prices that fixed per-record cost against
//! the DOCUMENT path, which pays it once per document.
//!
//! The pair alone cannot say *how much* of the LINES/DOCUMENT gap is that
//! per-record construction, so two more groups price the terms the LINES path
//! adds to the DOCUMENT path, each in isolation over the same fixture:
//!
//! - `json_lines_construct` builds and drops the reader `json_lines_scan`
//!   builds per record — `JsonStreamReader::new_custom` over a null source,
//!   no JSON, no framing. Criterion's element throughput is the record count,
//!   so its per-element time is the fixed lifecycle cost.
//! - `json_lines_framing_only` serves the same bytes through a bench-local
//!   mirror of `json::line::LineFraming`'s loop with no JSON parser at all —
//!   the byte-serving half of the LINES path on its own.
//!
//! Read together with `json_document_scan` (the same elements, one reader for
//! the whole document, no framing layer), the three terms decompose the LINES
//! total: DOCUMENT ≈ per-record pipeline + parse, and LINES ≈ DOCUMENT +
//! per-record construction + framing. The residual is the transport layer
//! difference (`BufReader` in, `BudgetedRead` out) plus the per-record
//! struson warm-up (the first `fill_buffer`).

use std::{
    hint::black_box,
    io::{self, BufRead, BufReader, Cursor, Read},
    time::Duration,
};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use struson::reader::{JsonStreamReader, ReaderSettings};
use tinio_select::{
    events::{InputFormat, SelectConfig, SelectEvent, select_iter},
    json,
    sql::{QueryPlan, parse},
};

#[path = "../tests/support/json_fixture.rs"]
mod json_fixture;

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
        if let SelectEvent::Stats(counters) = ev {
            scanned = counters.bytes_scanned;
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

/// Benchmark fixture rows (JSON). 897 751 bytes as generated, deliberately:
/// the doc must stay *under* the 1 MB whole-document cap so the pre-refactor
/// implementation can read it too — a bigger fixture would make the before/after
/// incomparable rather than merely slow.
const JSON_ELEMENTS: usize = 10_000;

/// JSON DOCUMENT scan: an array of small objects, streamed (2026-09-12).
fn json_document_scan(c: &mut Criterion) {
    let mut doc = String::from("[");
    for i in 0..JSON_ELEMENTS {
        if i > 0 {
            doc.push(',');
        }
        doc.push_str(&json_fixture::element_compact(i as u64));
    }
    doc.push(']');
    let doc = doc.into_bytes();
    let plan = parse("SELECT * FROM S3Object").unwrap();
    // Struct-update form, matching `events.rs:995-1001` — also what keeps
    // `clippy::field_reassign_with_default` quiet under `-D warnings`.
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params {
            ty: json::Type::Document,
        }),
        ..Default::default()
    };
    let mut group = c.benchmark_group("json_document_scan");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(doc.len() as u64));
    group.bench_function("select_star", |b| {
        b.iter_batched(
            || Box::new(Cursor::new(doc.clone())) as Box<dyn Read + Send>,
            |input| drain(plan.clone(), config.clone(), input),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// The LINES fixture: the DOCUMENT group's elements, one per line. 897 751
/// bytes from the elements plus one byte per record.
fn generate_lines() -> Vec<u8> {
    let mut lines = String::new();
    for i in 0..JSON_ELEMENTS {
        lines.push_str(&json_fixture::element_compact(i as u64));
        lines.push('\n');
    }
    lines.into_bytes()
}

/// JSON LINES scan: the same elements, one per line (2026-09-12). Everything
/// the DOCUMENT group measures is held equal — same fixture elements, same
/// plan, same throughput denominator basis, same `iter_batched` shape — so the
/// difference is the framing: a fresh `JsonStreamReader` per record, over the
/// `LineFraming` + `BufReader` pair instead of one reader per document.
fn json_lines_scan(c: &mut Criterion) {
    let lines = generate_lines();
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params {
            ty: json::Type::Lines,
        }),
        ..Default::default()
    };
    let mut group = c.benchmark_group("json_lines_scan");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(lines.len() as u64));
    group.bench_function("select_star", |b| {
        b.iter_batched(
            || Box::new(Cursor::new(lines.clone())) as Box<dyn Read + Send>,
            |input| drain(plan.clone(), config.clone(), input),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// The reader settings `json::source::settings()` builds, mirrored here because
/// that module is `pub(crate)`: all six fields written out, `track_path` off (we
/// never read the path) and `restrict_number_values` off (fact 11).
fn reader_settings() -> ReaderSettings {
    ReaderSettings {
        allow_comments: false,
        allow_trailing_comma: false,
        allow_multiple_top_level: false,
        track_path: false,
        max_nesting_depth: Some(128),
        restrict_number_values: false,
    }
}

/// The fixed per-record cost `json_lines_scan` pays and `json_document_scan`
/// pays once: `JsonStreamReader::new_custom` — `[u8; 1024]` zeroed in place plus
/// two `Vec::with_capacity` — and the drop that frees them. One construction per
/// record, over a null source, with no JSON and no framing, so what is left is
/// exactly the lifecycle cost (`consume_trailing_whitespace` consumes the
/// reader, so this is the whole of what a LINES record can save). Criterion's
/// element throughput is the record count.
fn json_lines_construct(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_lines_construct");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Elements(JSON_ELEMENTS as u64));
    group.bench_function("per_record", |b| {
        let mut source = io::empty();
        b.iter(|| {
            for _ in 0..JSON_ELEMENTS {
                let mut reader =
                    JsonStreamReader::new_custom(black_box(&mut source), reader_settings());
                black_box(&mut reader);
            }
        });
    });
    group.finish();
}

// Kept in sync by hand: this bench re-implements crate items it cannot import,
// all `pub(crate)` — `record::MAX_RECORD` / `record::CAP_MESSAGE` (the consts
// below), `json::line::{LineFraming, LineStart}` (the mirror loop below) and
// `json::source::settings()` (`reader_settings` above). Widening the crate's
// visibility for a benchmark is not worth it, so nothing links them.

/// `json::line`'s width cap (`record::MAX_RECORD`) and its message, mirrored
/// with the framing loop below.
const MAX_RECORD: u64 = 1024 * 1024;
const CAP_MESSAGE: &str = "input record exceeds 1 MB";

/// A bench-local mirror of `json::line::LineFraming` (`src/json/line.rs`,
/// `pub(crate)`), so the LINES framing can be priced without a JSON parser. The
/// serve loop is copied line for line: the eager `fill_buf` EOF probe in
/// `start_line`, one `fill_buf` / newline scan / `consume` per `read`, the
/// `out.len() + 1` window that keeps the scan off the buffered tail, and the
/// mid-serve span cap.
struct LineFraming<R> {
    inner: R,
    offset: u64,
    span: u64,
    eol: bool,
}

/// What a mirror `start_line` found: a line exists (carrying its start offset),
/// or clean end of input.
enum LineStart {
    Line(u64),
    Eof,
}

impl<R> LineFraming<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            offset: 0,
            span: 0,
            eol: true,
        }
    }
}

impl<R: BufRead> LineFraming<R> {
    fn start_line(&mut self) -> io::Result<LineStart> {
        if self.inner.fill_buf()?.is_empty() {
            return Ok(LineStart::Eof);
        }
        self.eol = false;
        self.span = 0;
        Ok(LineStart::Line(self.offset))
    }

    fn cap() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE)
    }
}

impl<R: BufRead> Read for LineFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.eol || out.is_empty() {
            return Ok(0);
        }
        let avail = self.inner.fill_buf()?;
        if avail.is_empty() {
            // Unterminated final line: EOF ends its content.
            self.eol = true;
            return Ok(0);
        }
        let w = avail.len().min(out.len() + 1);
        let nl = avail[..w].iter().position(|&b| b == b'\n');
        let content = nl.unwrap_or(avail.len());
        let n = content.min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.inner.consume(n);
        self.offset += n as u64;
        self.span += n as u64;
        if n == content && nl.is_some() {
            self.inner.consume(1);
            self.offset += 1;
            self.span += 1;
            self.eol = true;
        }
        if self.span > MAX_RECORD {
            return Err(Self::cap());
        }
        Ok(n)
    }
}

/// The LINES framing on its own: the same bytes, served one line at a time
/// through the mirror above, with no `JsonStreamReader` and no matcher. Each
/// line is drained the way struson drains one — reads of 1 KiB until the
/// framing reports EOF at the newline. This is a lower bound on `json_lines_scan`'s
/// non-parse cost (the real `LineFraming` also serves the reader's first
/// `fill_buffer` and the trailing probe's read).
fn json_lines_framing_only(c: &mut Criterion) {
    let lines = generate_lines();
    let mut group = c.benchmark_group("json_lines_framing_only");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(lines.len() as u64));
    group.bench_function("serve", |b| {
        b.iter_batched(
            || BufReader::new(Cursor::new(lines.clone())),
            |inner| {
                let mut framing = LineFraming::new(inner);
                let mut sink = [0u8; 1024];
                let mut last_start = 0u64;
                loop {
                    match framing.start_line().unwrap() {
                        LineStart::Eof => break,
                        LineStart::Line(start) => last_start = start,
                    }
                    while framing.read(&mut sink).unwrap() > 0 {}
                }
                black_box(last_start)
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    full_scan,
    filtered_scan,
    json_document_scan,
    json_lines_scan,
    json_lines_construct,
    json_lines_framing_only
);
criterion_main!(benches);
