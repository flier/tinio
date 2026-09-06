//! Event adapter (spec §1 `events.rs`): drives the record pipeline as the
//! synchronous `select_iter` event iterator.
//!
//! Per-pull composition: decompress (`decompressed`), count the uncompressed
//! bytes leaving the decoder (`bytes_scanned`), frame records per the input
//! format, apply the ScanRange window (`RangeFilter` wraps the reader),
//! evaluate (`Engine`), serialize per the output mode, and pack whole
//! serialized records into `Records` events capped at 1 MB — a record never
//! splits across events; a serialized record over 1 MB errors the stream
//! (`TooLarge`) after the pending buffer flush (review 2026-09-05 #10).
//! `Progress` events (when requested) are checked at flush points only —
//! 1 s elapsed or 1 MB scanned since the last `Progress` (pull-driven
//! timing: a slow consumer delays them; spec §1.1, accepted deviation).
//! `Cont` fires after `every_n` records and/or `idle` since the last
//! emitted event, checked at the same flush points. `Stats` + `End` are
//! emitted at EOF, after an aggregate finish row lands in the buffer —
//! except when the finish row itself serializes over 1 MB: the stream
//! errors `TooLarge` instead and no `Stats`/`End` follow. No async
//! anywhere — the async bridge is tinio-server's job (spec §1.1).

use std::cell::Cell;
use std::collections::VecDeque;
use std::io::Read;
use std::rc::Rc;
use std::time::{Duration, Instant};

#[cfg(feature = "parquet")]
use std::io::Cursor;

use crate::engine::Engine;
use crate::json::{JsonReader, JsonType};
use crate::output::{serialize_row, OutputMode};
use crate::record::{
    decompressed, Compression, CsvHeader, CsvParams, CsvReader, RangeFilter, RecordReader,
};
use crate::row::{display, Field, Record};
use crate::sql::QueryPlan;
use crate::SelectError;

#[cfg(feature = "parquet")]
use crate::parquet::ParquetReader;
#[cfg(feature = "parquet")]
use crate::sql::referenced_columns;

/// One stream item, format-agnostic — the server maps these to s3s events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectEvent {
    /// Whole serialized output records packed at record boundaries (≤ 1 MB).
    Records(Vec<u8>),
    /// Cadence report; only when `request_progress` was set.
    Progress {
        bytes_scanned: u64,
        bytes_processed: u64,
        bytes_returned: u64,
    },
    /// EOF summary; always emitted exactly once.
    Stats {
        bytes_scanned: u64,
        bytes_processed: u64,
        bytes_returned: u64,
    },
    /// Connection keepalive per the `ContPolicy`.
    Cont,
    /// Last item, always after `Stats`.
    End,
}

/// Input framing (spec §1: CSV / JSON / parquet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFormat {
    Csv(CsvParams),
    Json(JsonParams),
    Parquet(ParquetParams),
}

/// JSON input options (S3 Select `InputSerialization.JSON`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonParams {
    pub ty: JsonType,
}

/// Parquet input options: the SELECT/WHERE column name set for projection
/// pruning. Empty = every schema column (`Wild`/reference-free plans).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetParams {
    pub projection: Vec<String>,
}

/// Continuation-event policy (grilling Q3). `idle` = send `Cont` after that
/// duration with no emitted event; `every_n` = send `Cont` after that many
/// records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContPolicy {
    pub idle: Option<Duration>,
    pub every_n: Option<usize>,
}

impl Default for ContPolicy {
    /// Grilling Q3 defaults: 5 s idle, 4096 records (tinio-server's pick).
    fn default() -> Self {
        Self {
            idle: Some(Duration::from_secs(5)),
            every_n: Some(4096),
        }
    }
}

/// Parquet memory bound (review 2026-09-05): the whole object is buffered;
/// the default is the server's request-level 400 ceiling.
pub const MAX_PARQUET_BYTES: u64 = 256 * 1024 * 1024;

/// One select request: input framing + output mode + pipeline knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectConfig {
    pub input_format: InputFormat,
    pub output: OutputMode,
    pub compression: Compression,
    /// ScanRange window (resolved, validated by the server): a record counts
    /// when its first byte falls in `[start, end]`; inner `None` = no upper
    /// bound. `end`-only resolution (`start = size - end`) is the caller's
    /// (it owns the object size, R3 — this tuple is the resolved window).
    pub scan_range: Option<(u64, Option<u64>)>,
    /// RequestProgress — `Progress` cadence 1 s / 1 MB (grilling Q2).
    pub request_progress: bool,
    pub cont: ContPolicy,
    /// Stored-object size — the caller's source for ScanRange arithmetic.
    pub size: Option<u64>,
    pub max_parquet_bytes: u64,
}

impl Default for SelectConfig {
    fn default() -> Self {
        Self {
            // AWS documented defaults for unset input fields (review 2026-09-05).
            input_format: InputFormat::Csv(CsvParams {
                field_delimiter: b',',
                record_delimiter: b'\n',
                quote: b'"',
                escape: b'"',
                comments: None,
                header: CsvHeader::None_,
                allow_quoted_record_delimiter: true,
            }),
            output: OutputMode::Csv(Default::default()),
            compression: Compression::None_,
            scan_range: None,
            request_progress: false,
            cont: ContPolicy::default(),
            size: None,
            max_parquet_bytes: MAX_PARQUET_BYTES,
        }
    }
}

/// One select request driven to its `Stats` + `End` by pull; `input` is a
/// blocking reader over the stored object (decompression happens inside for
/// CSV/JSON; parquet is the caller-buffered object). Errors are items —
/// `Parse`/`Unsupported` are request-level (400 before streaming), the rest
/// abort the stream.
pub fn select_iter(
    plan: QueryPlan,
    config: SelectConfig,
    input: Box<dyn Read + Send>,
) -> impl Iterator<Item = Result<SelectEvent, SelectError>> {
    SelectIter::new(plan, config, input)
}

/// One serialized record over 1 MB — the output side of the 1 MB cap.
const EVENT_CAP: u64 = 1024 * 1024;

/// Bytes counter shared with the counting source (below the format readers):
/// `scanned` = uncompressed bytes consumed from the decoder.
type Scanned = Rc<Cell<u64>>;

/// `Read` wrapper that counts bytes leaving the decoder into the reader —
/// the `bytes_scanned` measurement point (spec Decisions).
struct CountingRead<R: Read> {
    inner: R,
    count: Scanned,
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.set(self.count.get() + n as u64);
        Ok(n)
    }
}

/// The adapter's state machine. `next()` pumps records into the 1 MB output
/// buffer and drains an internal queue of events to emit; every event is
/// delivered in order: `Progress` (cadence) → `Records` → `Cont` (policy).
struct SelectIter {
    reader: Option<Box<dyn RecordReader>>,
    engine: Engine,
    mode: OutputMode,
    buffer: Vec<u8>,
    scanned: Scanned,
    processed: u64,
    returned: u64,
    /// Uncompressed start of the record whose payload span is still open.
    open_start: Option<u64>,
    /// Record-span counting applies to CSV/JSON; parquet rows count
    /// per-row text instead (its reader has no offsets).
    use_spans: bool,
    every_n: Option<usize>,
    n_since_cont: usize,
    cont_due: bool,
    idle: Option<Duration>,
    request_progress: bool,
    last_progress_at: Instant,
    last_progress_scan: u64,
    last_emit: Instant,
    queue: VecDeque<SelectEvent>,
    error: Option<SelectError>,
    completed: bool,
    done: bool,
}

impl SelectIter {
    fn new(plan: QueryPlan, config: SelectConfig, input: Box<dyn Read + Send>) -> Self {
        let scanned = Rc::new(Cell::new(0));
        let (reader, error) = match build_reader(&plan, &config, input, scanned.clone()) {
            Ok(reader) => (Some(reader), None),
            Err(e) => (None, Some(e)),
        };
        let now = Instant::now();
        Self {
            reader,
            engine: Engine::new(plan),
            mode: config.output,
            buffer: Vec::new(),
            scanned,
            processed: 0,
            returned: 0,
            open_start: None,
            use_spans: !matches!(config.input_format, InputFormat::Parquet(_)),
            every_n: config.cont.every_n,
            n_since_cont: 0,
            cont_due: false,
            idle: config.cont.idle,
            request_progress: config.request_progress,
            last_progress_at: now,
            last_progress_scan: 0,
            last_emit: now,
            queue: VecDeque::new(),
            error,
            completed: false,
            done: false,
        }
    }

    fn pump(&mut self) {
        if self.error.is_some() || self.completed {
            return;
        }
        if let Err(e) = self.step() {
            self.error = Some(e);
        }
    }

    /// One record cycle: pull a record, account its payload, evaluate,
    /// serialize into the buffer, flush at the 1 MB boundary.
    fn step(&mut self) -> Result<(), SelectError> {
        let (item, at) = {
            let reader = self.reader.as_mut().expect("reader present");
            (reader.next()?, reader.last_record_start())
        };
        match item {
            Some(rec) => {
                // Payload spans close when the next record's start advances;
                // the final span closes in `eof`. Dropped pre-range records
                // sit before `open_start` — counted as scanned only, never
                // processed; the past-window stop record is excluded by the
                // `at`-close in `eof`.
                if self.use_spans {
                    match self.open_start {
                        Some(prev) => {
                            self.processed += at.saturating_sub(prev);
                            self.open_start = Some(at);
                        }
                        None => self.open_start = Some(at),
                    }
                }
                if let Record::Parquet(fields, _) = &rec {
                    // "row bytes before serialization" = the decoded field
                    // text total — parquet readers carry no offsets.
                    self.processed += fields
                        .iter()
                        .map(|f| match f {
                            Field::Present(v) => display(v).len() as u64,
                            Field::Missing => 0,
                        })
                        .sum::<u64>();
                }
                self.n_since_cont += 1;
                if let Some(n) = self.every_n
                    && self.n_since_cont >= n
                {
                    self.n_since_cont = 0;
                    self.cont_due = true;
                }
                if let Some(row) = self.engine.next(rec)? {
                    let bytes = serialize_row(&self.mode, &row)?;
                    self.buffer_row(bytes)?;
                }
                Ok(())
            }
            None => self.eof(at),
        }
    }

    /// One serialized row into the buffer. Flush ordering (review
    /// 2026-09-05 #10): the pending buffer flushes before an over-cap record
    /// errors `TooLarge` — records never split, never span events.
    fn buffer_row(&mut self, bytes: Vec<u8>) -> Result<(), SelectError> {
        if bytes.len() as u64 > EVENT_CAP {
            if !self.buffer.is_empty() {
                self.flush();
            }
            return Err(SelectError::TooLarge);
        }
        if bytes.len() as u64 + self.buffer.len() as u64 > EVENT_CAP {
            self.flush();
        }
        self.buffer.extend_from_slice(&bytes);
        if self.buffer.len() as u64 >= EVENT_CAP {
            self.flush();
        }
        Ok(())
    }

    /// Emit-point flush: gather cadence events around the `Records` event.
    fn flush(&mut self) {
        let bytes = std::mem::take(&mut self.buffer);
        self.returned += bytes.len() as u64;
        let scanned = self.scanned.get();
        if self.request_progress
            && (scanned.saturating_sub(self.last_progress_scan) >= EVENT_CAP
                || self.last_progress_at.elapsed() >= Duration::from_secs(1))
        {
            self.queue.push_back(SelectEvent::Progress {
                bytes_scanned: scanned,
                bytes_processed: self.processed,
                bytes_returned: self.returned,
            });
            self.last_progress_scan = scanned;
            self.last_progress_at = Instant::now();
        }
        self.queue.push_back(SelectEvent::Records(bytes));
        if self.cont_due {
            self.cont_due = false;
            self.queue.push_back(SelectEvent::Cont);
        } else if let Some(idle) = self.idle
            && self.last_emit.elapsed() >= idle
        {
            self.queue.push_back(SelectEvent::Cont);
        }
    }

    /// EOF: close the open span, flush the aggregate finish row with the
    /// same buffer rules, then `Stats` + `End` — exactly once, unless the
    /// finish row's flush errors `TooLarge` (1 MB) first, after which the
    /// stream stops on that error.
    /// `at` is the offset captured at the final pull: the past-window record's
    /// start when `RangeFilter` stopped the stream, else the last yielded
    /// record's own start — close at `at` in the first case (the consumed
    /// total would include the stop record), at the consumed total otherwise
    /// (full scan / window run to EOF: the span ends at the consumed end).
    fn eof(&mut self, at: u64) -> Result<(), SelectError> {
        self.completed = true;
        if self.use_spans
            && let Some(start) = self.open_start.take()
        {
            let end = if at > start { at } else { self.scanned.get() };
            self.processed += end.saturating_sub(start);
        }
        if let Some(row) = self.engine.finish()? {
            let bytes = serialize_row(&self.mode, &row)?;
            self.buffer_row(bytes)?;
        }
        if !self.buffer.is_empty() {
            self.flush();
        }
        self.queue.push_back(SelectEvent::Stats {
            bytes_scanned: self.scanned.get(),
            bytes_processed: self.processed,
            bytes_returned: self.returned,
        });
        self.queue.push_back(SelectEvent::End);
        Ok(())
    }
}

impl Iterator for SelectIter {
    type Item = Result<SelectEvent, SelectError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        while self.queue.is_empty() && self.error.is_none() && !self.completed {
            self.pump();
        }
        if let Some(ev) = self.queue.pop_front() {
            // Clock of the last delivered event — the `Cont` idle window.
            self.last_emit = Instant::now();
            return Some(Ok(ev));
        }
        if let Some(e) = self.error.take() {
            self.done = true;
            return Some(Err(e));
        }
        self.done = true;
        None
    }
}

/// Pipeline composition per R2: the `RangeFilter` wraps the format reader
/// (record-start offsets are the reader's own accounting); the counting
/// source sits between the decoder and the reader.
fn build_reader(
    plan: &QueryPlan,
    config: &SelectConfig,
    input: Box<dyn Read + Send>,
    scanned: Scanned,
) -> Result<Box<dyn RecordReader>, SelectError> {
    fn count(inner: Box<dyn Read + Send>, scanned: &Scanned) -> CountingRead<Box<dyn Read + Send>> {
        CountingRead {
            inner,
            count: scanned.clone(),
        }
    }
    let base: Box<dyn RecordReader> = match &config.input_format {
        InputFormat::Csv(params) => Box::new(CsvReader::new(
            count(decompressed(config.compression, input), &scanned),
            params.clone(),
        )),
        InputFormat::Json(params) => Box::new(JsonReader::new(
            count(decompressed(config.compression, input), &scanned),
            params.ty,
            &plan.from,
        )),
        #[cfg(feature = "parquet")]
        InputFormat::Parquet(params) => {
            // Parquet: whole-object slurp — no seek in the storage path; the
            // bound is checked request-level by the server and again here.
            let mut source = count(input, &scanned);
            let mut buf = Vec::new();
            source.read_to_end(&mut buf)?;
            if buf.len() as u64 > config.max_parquet_bytes {
                return Err(SelectError::ParquetTooLarge);
            }
            let projection = if params.projection.is_empty() {
                referenced_columns(plan)
            } else {
                params.projection.clone()
            };
            Box::new(ParquetReader::new(
                Cursor::new(buf),
                projection,
                config.max_parquet_bytes,
            )?)
        }
        #[cfg(not(feature = "parquet"))]
        InputFormat::Parquet(_) => {
            return Err(SelectError::Unsupported(
                "parquet input requires the parquet feature".into(),
            ));
        }
    };
    Ok(match config.scan_range {
        Some((start, end)) => Box::new(RangeFilter::new(base, start, end)),
        None => base,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::json::JsonType;
    use crate::output::JsonOutputParams;
    use crate::record::CsvHeader;
    use crate::sql::parse;

    use super::*;

    /// AWS-ish CSV params (`,`/`\n`/`"`/`"`, no comments) — the record.rs
    /// test-helper shape for the given header mode.
    fn params(header: CsvHeader) -> CsvParams {
        CsvParams {
            field_delimiter: b',',
            record_delimiter: b'\n',
            quote: b'"',
            escape: b'"',
            comments: None,
            header,
            allow_quoted_record_delimiter: true,
        }
    }

    /// The whole event item list for `sql` over `input`.
    fn run(sql: &str, config: SelectConfig, input: &[u8]) -> Vec<Result<SelectEvent, SelectError>> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        select_iter(plan, config, Box::new(Cursor::new(input.to_vec()))).collect()
    }

    /// One data row of `n` `a`s + `\n` (a `Records` payload form).
    fn row(n: usize) -> Vec<u8> {
        let mut r = vec![b'a'; n];
        r.push(b'\n');
        r
    }

    #[test]
    fn three_rows_streams_records_stats_end() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, b"1,alice\n2,bob\n3,carol\n");
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"1,alice\n2,bob\n3,carol\n".to_vec())),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 22,
                    bytes_processed: 22,
                    bytes_returned: 22,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn empty_input_stats_zeros_then_end() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, b"");
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Stats {
                    bytes_scanned: 0,
                    bytes_processed: 0,
                    bytes_returned: 0,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn aggregate_finish_row_lands_before_stats() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            ..Default::default()
        };
        let events = run("SELECT count(*) FROM S3Object s", config, b"1,alice\n2,bob\n3,carol\n");
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"3\n".to_vec())),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 22,
                    bytes_processed: 22,
                    bytes_returned: 2,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn single_over_1mb_serialized_record_errors_too_large() {
        // Input span (content + terminator) is 1048575 (< the reader cap);
        // JSON output inflates it past the 1 MB event cap → `TooLarge`.
        let mut input = vec![b'a'; 1024 * 1024 - 2];
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(events, vec![Err(SelectError::TooLarge)]);
    }

    #[test]
    fn over_1mb_output_record_errors_after_boundary_flush() {
        // The flush of the buffered small row precedes the `TooLarge` error
        // ("flush first" — review 2026-09-05 #10 ordering).
        let mut input = b"x\n".to_vec();
        input.extend_from_slice(&[b'a'; 1024 * 1024 - 2]);
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut it = select_iter(plan, config, Box::new(Cursor::new(input)));
        assert_eq!(
            it.next(),
            Some(Ok(SelectEvent::Records(b"{\"_1\":\"x\"}\n".to_vec())))
        );
        assert_eq!(it.next(), Some(Err(SelectError::TooLarge)));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn input_record_over_1mb_surfaces_reader_cap_error() {
        // The reader's own 1 MB input cap fires first — `Format`, not
        // `TooLarge` (output side). Documenting the split.
        let mut input = vec![b'a'; 1024 * 1024];
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(
            events,
            vec![Err(SelectError::Format("input record exceeds 1 MB".into()))]
        );
    }

    #[test]
    fn near_1mb_record_flushed_whole_at_boundary() {
        // 1048575-byte row: it must appear as ONE Records event (the boundary
        // flush happens when the next row would push past 1 MB) — never split.
        let mut input = vec![b'a'; 1024 * 1024 - 2];
        input.push(b'\n');
        input.extend_from_slice(b"x\n");
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(events.len(), 4);
        assert_eq!(events[0], Ok(SelectEvent::Records(row(1024 * 1024 - 2))));
        assert_eq!(events[1], Ok(SelectEvent::Records(b"x\n".to_vec())));
        assert_eq!(
            events[2],
            Ok(SelectEvent::Stats {
                bytes_scanned: 1_048_577,
                bytes_processed: 1_048_577,
                bytes_returned: 1_048_577,
            })
        );
        assert_eq!(events[3], Ok(SelectEvent::End));
    }

    #[test]
    fn cont_every_n_records_between_records_events() {
        // three ~600 KB rows: each flush boundary emits its own Records, and
        // the Cont lands right after the Records event whose processing crossed
        // the second record since the last Cont.
        let mut input = Vec::new();
        for _ in 0..3 {
            input.extend_from_slice(&[b'a'; 600_000]);
            input.push(b'\n');
        }
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            cont: ContPolicy {
                idle: None,
                every_n: Some(2),
            },
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(row(600_000))),
                Ok(SelectEvent::Cont),
                Ok(SelectEvent::Records(row(600_000))),
                Ok(SelectEvent::Records(row(600_000))),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 1_800_003,
                    bytes_processed: 1_800_003,
                    bytes_returned: 1_800_003,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn cont_after_idle_emits_following_records() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            cont: ContPolicy {
                idle: Some(Duration::ZERO),
                every_n: None,
            },
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, b"1,alice\n2,bob\n");
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"1,alice\n2,bob\n".to_vec())),
                Ok(SelectEvent::Cont),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 14,
                    bytes_processed: 14,
                    bytes_returned: 14,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn progress_counters_advance_at_flush_points() {
        // Three ~400 KB rows (1.2 MB total): the first flush crosses the 1 MB
        // progress threshold, so a Progress event reports running counters
        // ahead of the Records event it precedes.
        let mut input = Vec::new();
        for _ in 0..3 {
            input.extend_from_slice(&[b'a'; 400_000]);
            input.push(b'\n');
        }
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            request_progress: true,
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        // First flush: rows 1+2 together (800_002 bytes); row 3 alone.
        let mut two = vec![b'a'; 400_000];
        two.push(b'\n');
        two.extend_from_slice(&[b'a'; 400_000]);
        two.push(b'\n');
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Progress {
                    bytes_scanned: 1_200_003,
                    bytes_processed: 800_002,
                    bytes_returned: 800_002,
                }),
                Ok(SelectEvent::Records(two)),
                Ok(SelectEvent::Records(row(400_000))),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 1_200_003,
                    bytes_processed: 1_200_003,
                    bytes_returned: 1_200_003,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn json_lines_streams_records_stats_end() {
        let config = SelectConfig {
            input_format: InputFormat::Json(JsonParams { ty: JsonType::Lines }),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let events = run(
            "SELECT * FROM S3Object",
            config,
            b"{\"a\": 1}\n{\"b\": 2}\n",
        );
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"{\"a\":1}\n{\"b\":2}\n".to_vec())),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 18,
                    bytes_processed: 18,
                    bytes_returned: 16,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn scan_range_window_counts_only_window_records() {
        // Window [13, 16] over the T10 fixture: r1@13, r2@16 in range; r3@19
        // stops the stream. `bytes_processed` closes the last span at the
        // stop record's start (3 + 3 = 6), never at the consumed total (22).
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(CsvHeader::None_)),
            scan_range: Some((13, Some(16))),
            ..Default::default()
        };
        let events = run(
            "SELECT * FROM S3Object",
            config,
            b"0123456789ab\nr1\nr2\nr3\n",
        );
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"r1\nr2\n".to_vec())),
                Ok(SelectEvent::Stats {
                    bytes_scanned: 22,
                    bytes_processed: 6,
                    bytes_returned: 6,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_streams_records_stats_end() {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let config = SelectConfig {
            input_format: InputFormat::Parquet(ParquetParams {
                projection: Vec::new(),
            }),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let events = run("SELECT name FROM S3Object s", config, &buf);
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n".to_vec())),
                Ok(SelectEvent::Stats {
                    bytes_scanned: buf.len() as u64,
                    bytes_processed: 8, // "alice" + "bob" row text
                    bytes_returned: 32,
                }),
                Ok(SelectEvent::End),
            ]
        );
    }
}
