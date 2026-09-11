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

#[cfg(feature = "parquet")]
use std::io::Cursor;
use std::{
    cell::Cell,
    collections::VecDeque,
    io::Read,
    rc::Rc,
    time::{Duration, Instant},
};

use smart_default::SmartDefault;

#[cfg(feature = "parquet")]
use crate::parquet::ParquetReader;
#[cfg(feature = "parquet")]
use crate::sql::referenced_columns;
use crate::{
    csv,
    engine::Engine,
    error::Error,
    json,
    output::{OutputMode, serialize_row},
    record::{Compression, RangeFilter, RecordReader, ScanRange, decompressed},
    row::{Field, Record, Value, display},
    sql::QueryPlan,
};

/// The three stream counters carried by `Progress` and `Stats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteCounters {
    pub bytes_scanned: u64,
    pub bytes_processed: u64,
    pub bytes_returned: u64,
}

/// One stream item, format-agnostic — the server maps these to s3s events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectEvent {
    /// Whole serialized output records packed at record boundaries (≤ 1 MB).
    Records(Vec<u8>),
    /// Cadence report; only when `request_progress` was set.
    Progress(ByteCounters),
    /// EOF summary; always emitted exactly once.
    Stats(ByteCounters),
    /// Connection keepalive per the `ContPolicy`.
    Cont,
    /// Last item, always after `Stats`.
    End,
}

/// Input framing (spec §1: CSV / JSON / parquet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFormat {
    Csv(csv::Params),
    Json(json::Params),
    Parquet(ParquetParams),
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
///
/// Defaults: 5 s idle, 4096 records (tinio-server's pick).
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
pub struct ContPolicy {
    #[default(Some(Duration::from_secs(5)))]
    pub idle: Option<Duration>,
    #[default(Some(4096))]
    pub every_n: Option<usize>,
}

/// Parquet memory bound (review 2026-09-05): the whole object is buffered;
/// the default is the server's request-level 400 ceiling.
pub const MAX_PARQUET_BYTES: u64 = 256 * 1024 * 1024;

/// One select request: input framing + output mode + pipeline knobs.
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault)]
pub struct SelectConfig {
    /// AWS documented defaults for unset input fields (review 2026-09-05).
    #[default(InputFormat::Csv(csv::Params::default()))]
    pub input_format: InputFormat,
    #[default(OutputMode::Csv(Default::default()))]
    pub output: OutputMode,
    pub compression: Option<Compression>,
    /// ScanRange window (resolved, validated by the server): a record counts
    /// when its first byte falls in `[start, end]`; `end = None` = no upper
    /// bound. `end`-only resolution (`start = size - end`) is the caller's.
    pub scan_range: Option<ScanRange>,
    /// RequestProgress — `Progress` cadence 1 s / 1 MB (grilling Q2).
    pub request_progress: bool,
    pub cont: ContPolicy,
    #[default(MAX_PARQUET_BYTES)]
    pub max_parquet_bytes: u64,
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
) -> impl Iterator<Item = Result<SelectEvent, Error>> {
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

/// How `processed` advances: closed uncompressed record spans (CSV/JSON)
/// or decoded field-text totals (parquet — its reader has no offsets).
enum CountMode {
    Spans { open: Option<u64> },
    RowText,
}

/// The three stream counters plus payload accounting (`Progress` / `Stats`).
struct Tally {
    scanned: Scanned,
    processed: u64,
    returned: u64,
    mode: CountMode,
}

impl Tally {
    fn new(scanned: Scanned, spans: bool) -> Self {
        Self {
            scanned,
            processed: 0,
            returned: 0,
            mode: if spans {
                CountMode::Spans { open: None }
            } else {
                CountMode::RowText
            },
        }
    }

    fn snapshot(&self) -> ByteCounters {
        ByteCounters {
            bytes_scanned: self.scanned.get(),
            bytes_processed: self.processed,
            bytes_returned: self.returned,
        }
    }

    fn scanned(&self) -> u64 {
        self.scanned.get()
    }

    fn spans(&self) -> bool {
        matches!(self.mode, CountMode::Spans { .. })
    }

    /// Close the previous record's span at `at` (CSV/JSON) or add the
    /// parquet row's decoded text; dropped pre-range records sit before
    /// the first open start and never become processed.
    fn account(&mut self, at: u64, rec: &Record) {
        match (&mut self.mode, rec) {
            (CountMode::Spans { open }, _) => match *open {
                Some(prev) => {
                    self.processed += at.saturating_sub(prev);
                    *open = Some(at);
                }
                None => *open = Some(at),
            },
            (CountMode::RowText, Record::Parquet(cols)) => {
                self.processed += cols
                    .fields
                    .iter()
                    .map(|f| match f {
                        Field::Present(v) => display(v).len() as u64,
                        Field::Missing => 0,
                    })
                    .sum::<u64>();
            }
            (CountMode::RowText, _) => {}
        }
    }

    /// EOF close: the past-window stop record's start when `at` is past
    /// the open span, else the consumed total (full scan / window to EOF).
    fn close(&mut self, at: u64) {
        let CountMode::Spans { open } = &mut self.mode else {
            return;
        };
        let Some(start) = open.take() else {
            return;
        };
        let end = if at > start { at } else { self.scanned.get() };
        self.processed += end.saturating_sub(start);
    }

    fn add_returned(&mut self, n: u64) {
        self.returned += n;
    }
}

/// Progress (1 s / 1 MB), Cont (`ContPolicy`), silent-scan keepalive (X4).
struct Cadence {
    request_progress: bool,
    last_progress_at: Instant,
    last_progress_scan: u64,
    last_keepalive: u64,
    every_n: Option<usize>,
    n_since_cont: usize,
    cont_due: bool,
    idle: Option<Duration>,
    last_emit: Instant,
}

impl Cadence {
    fn new(request_progress: bool, cont: ContPolicy, now: Instant) -> Self {
        Self {
            request_progress,
            last_progress_at: now,
            last_progress_scan: 0,
            last_keepalive: 0,
            every_n: cont.every_n,
            n_since_cont: 0,
            cont_due: false,
            idle: cont.idle,
            last_emit: now,
        }
    }

    fn on_record(&mut self) {
        self.n_since_cont += 1;
        if let Some(n) = self.every_n
            && self.n_since_cont >= n
        {
            self.n_since_cont = 0;
            self.cont_due = true;
        }
    }

    fn take_progress(&mut self, scanned: u64) -> bool {
        if !self.request_progress {
            return false;
        }
        if scanned.saturating_sub(self.last_progress_scan) < EVENT_CAP
            && self.last_progress_at.elapsed() < Duration::from_secs(1)
        {
            return false;
        }
        self.last_progress_scan = scanned;
        self.last_progress_at = Instant::now();
        true
    }

    fn take_cont(&mut self) -> bool {
        if self.cont_due {
            self.cont_due = false;
            true
        } else if let Some(idle) = self.idle
            && self.last_emit.elapsed() >= idle
        {
            true
        } else {
            false
        }
    }

    /// Silent-scan 1 MB keepalive. `Some(progress)` = emit Cont, and
    /// Progress iff the bool is set.
    fn take_keepalive(&mut self, scanned: u64, quiet: bool) -> Option<bool> {
        if !quiet || scanned.saturating_sub(self.last_keepalive) < EVENT_CAP {
            return None;
        }
        self.last_keepalive = scanned;
        let progress = self.request_progress;
        if progress {
            self.last_progress_scan = scanned;
            self.last_progress_at = Instant::now();
        }
        Some(progress)
    }

    fn on_emit(&mut self) {
        self.last_emit = Instant::now();
    }
}

/// Frames yielded by `Packer::stage` — at most two (pending-then-cap).
struct Staged {
    frames: Vec<Vec<u8>>,
    too_large: bool,
}

/// Whole-record packing into ≤ 1 MB frames. A record never splits.
struct Packer {
    buf: Vec<u8>,
}

impl Packer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Stage one serialized record. Frames that must leave now come first;
    /// `too_large` after any pending flush (review 2026-09-05 #10).
    fn stage(&mut self, bytes: Vec<u8>) -> Staged {
        let mut staged = Staged {
            frames: Vec::new(),
            too_large: false,
        };
        if bytes.len() as u64 > EVENT_CAP {
            if !self.buf.is_empty() {
                staged.frames.push(std::mem::take(&mut self.buf));
            }
            staged.too_large = true;
            return staged;
        }
        if bytes.len() as u64 + self.buf.len() as u64 > EVENT_CAP {
            staged.frames.push(std::mem::take(&mut self.buf));
        }
        self.buf.extend_from_slice(&bytes);
        if self.buf.len() as u64 >= EVENT_CAP {
            staged.frames.push(std::mem::take(&mut self.buf));
        }
        staged
    }

    fn take(&mut self) -> Option<Vec<u8>> {
        (!self.buf.is_empty()).then(|| std::mem::take(&mut self.buf))
    }
}

/// Delivery queue + stream lifecycle (error / completed / done).
struct Outbox {
    queue: VecDeque<SelectEvent>,
    error: Option<Error>,
    completed: bool,
    done: bool,
}

impl Outbox {
    fn new(error: Option<Error>) -> Self {
        Self {
            queue: VecDeque::new(),
            error,
            completed: false,
            done: false,
        }
    }

    fn push(&mut self, ev: SelectEvent) {
        self.queue.push_back(ev);
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn fail(&mut self, e: Error) {
        self.error = Some(e);
    }

    fn complete(&mut self) {
        self.completed = true;
    }

    fn is_completed(&self) -> bool {
        self.completed
    }

    fn has_error(&self) -> bool {
        self.error.is_some()
    }

    fn is_done(&self) -> bool {
        self.done
    }

    fn needs_pump(&self) -> bool {
        !self.done && self.queue.is_empty() && self.error.is_none() && !self.completed
    }

    fn pop(&mut self) -> Option<SelectEvent> {
        self.queue.pop_front()
    }

    fn take_error(&mut self) -> Option<Error> {
        let e = self.error.take()?;
        self.done = true;
        Some(e)
    }

    fn finish(&mut self) {
        self.done = true;
    }
}

/// The adapter's state machine. Pipeline (reader / engine / mode) plus
/// composed helpers: payload `Tally`, emit `Cadence`, record `Packer`,
/// delivery `Outbox`. `next()` pumps records into the packer and drains
/// the outbox; every event is delivered in order: `Progress` (cadence) →
/// `Records` → `Cont` (policy).
struct SelectIter {
    reader: Option<Box<dyn RecordReader>>,
    engine: Engine,
    mode: OutputMode,
    tally: Tally,
    cadence: Cadence,
    packer: Packer,
    outbox: Outbox,
}

impl SelectIter {
    fn new(plan: QueryPlan, config: SelectConfig, input: Box<dyn Read + Send>) -> Self {
        let SelectConfig {
            input_format,
            output,
            compression,
            scan_range,
            request_progress,
            cont,
            max_parquet_bytes,
        } = config;
        let use_spans = !matches!(input_format, InputFormat::Parquet(_));
        let scanned = Rc::new(Cell::new(0));
        let (reader, error) = match build_reader(
            &plan,
            input_format,
            compression,
            scan_range,
            max_parquet_bytes,
            input,
            scanned.clone(),
        ) {
            Ok(reader) => (Some(reader), None),
            Err(e) => (None, Some(e)),
        };
        let now = Instant::now();
        Self {
            reader,
            engine: Engine::new(plan),
            mode: output,
            tally: Tally::new(scanned, use_spans),
            cadence: Cadence::new(request_progress, cont, now),
            packer: Packer::new(),
            outbox: Outbox::new(error),
        }
    }

    fn pump(&mut self) {
        if self.outbox.has_error() || self.outbox.is_completed() {
            return;
        }
        if let Err(e) = self.step() {
            self.outbox.fail(e);
        }
    }

    /// One record cycle: pull a record, account its payload, evaluate,
    /// serialize into the packer, flush at the 1 MB boundary.
    fn step(&mut self) -> Result<(), Error> {
        // X1: a reached LIMIT stops the scan NOW (jump to the eof path) —
        // pulling `Ok(None)` as if filtered would scroll the whole object.
        if self.engine.limit_reached() {
            return self.eof(0);
        }
        let (item, at) = {
            let reader = self.reader.as_mut().expect("reader present");
            (reader.next()?, reader.last_record_start())
        };
        match item {
            Some(rec) => {
                self.tally.account(at, &rec);
                self.cadence.on_record();
                if let Some(row) = self.engine.next(rec)? {
                    // Parquet nested (list/struct → `Value::Json`) under CSV
                    // output is the nested-column error (grilling Q10); JSON
                    // nested renders as the compact-cell (SELECT * matrix).
                    if !self.tally.spans()
                        && matches!(self.mode, OutputMode::Csv(_))
                        && row
                            .vals
                            .iter()
                            .any(|f| matches!(f, Field::Present(Value::Json(_))))
                    {
                        return Err(Error::NestedCsv);
                    }
                    let bytes = serialize_row(&self.mode, &row)?;
                    self.buffer_row(bytes)?;
                }
                // X4: a scan that matches nothing streams nothing — the
                // client sees a silent connection and (since `blocking_send`
                // is the only cancellation probe) a disconnect goes unnoticed
                // until EOF. A keepalive cadence fires every scanned MB even
                // with an empty buffer — the client gets early frames and
                // the producer probes the out channel. Gated on both queues
                // being empty so it never interleaves with a real flush.
                if let Some(progress) = self.cadence.take_keepalive(
                    self.tally.scanned(),
                    self.packer.is_empty() && self.outbox.is_empty(),
                ) {
                    if progress {
                        self.outbox
                            .push(SelectEvent::Progress(self.tally.snapshot()));
                    }
                    self.outbox.push(SelectEvent::Cont);
                }
                Ok(())
            }
            None => self.eof(at),
        }
    }

    /// One serialized row into the packer. Flush ordering (review
    /// 2026-09-05 #10): the pending buffer flushes before an over-cap record
    /// errors `TooLarge` — records never split, never span events.
    fn buffer_row(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        let staged = self.packer.stage(bytes);
        for frame in staged.frames {
            self.flush_frame(frame);
        }
        if staged.too_large {
            Err(Error::TooLarge)
        } else {
            Ok(())
        }
    }

    /// Emit-point flush: gather cadence events around the `Records` event.
    fn flush_frame(&mut self, bytes: Vec<u8>) {
        self.tally.add_returned(bytes.len() as u64);
        if self.cadence.take_progress(self.tally.scanned()) {
            self.outbox
                .push(SelectEvent::Progress(self.tally.snapshot()));
        }
        self.outbox.push(SelectEvent::Records(bytes));
        if self.cadence.take_cont() {
            self.outbox.push(SelectEvent::Cont);
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
    fn eof(&mut self, at: u64) -> Result<(), Error> {
        self.outbox.complete();
        self.tally.close(at);
        if let Some(row) = self.engine.finish()? {
            let bytes = serialize_row(&self.mode, &row)?;
            self.buffer_row(bytes)?;
        }
        if let Some(bytes) = self.packer.take() {
            self.flush_frame(bytes);
        }
        self.outbox.push(SelectEvent::Stats(self.tally.snapshot()));
        self.outbox.push(SelectEvent::End);
        Ok(())
    }
}

impl Iterator for SelectIter {
    type Item = Result<SelectEvent, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.outbox.is_done() {
            return None;
        }
        while self.outbox.needs_pump() {
            self.pump();
        }
        if let Some(ev) = self.outbox.pop() {
            // Clock of the last delivered event — the `Cont` idle window.
            self.cadence.on_emit();
            return Some(Ok(ev));
        }
        if let Some(e) = self.outbox.take_error() {
            return Some(Err(e));
        }
        self.outbox.finish();
        None
    }
}

/// Pipeline composition per R2: the `RangeFilter` wraps the format reader
/// (record-start offsets are the reader's own accounting); the counting
/// source sits between the decoder and the reader.
fn build_reader(
    plan: &QueryPlan,
    input_format: InputFormat,
    compression: Option<Compression>,
    scan_range: Option<ScanRange>,
    max_parquet_bytes: u64,
    input: Box<dyn Read + Send>,
    scanned: Scanned,
) -> Result<Box<dyn RecordReader>, Error> {
    fn count(inner: Box<dyn Read + Send>, scanned: Scanned) -> CountingRead<Box<dyn Read + Send>> {
        CountingRead {
            inner,
            count: scanned,
        }
    }
    let base: Box<dyn RecordReader> = match input_format {
        InputFormat::Csv(params) => Box::new(csv::Reader::new(
            count(decompressed(compression, input), scanned),
            params,
        )),
        InputFormat::Json(params) => Box::new(json::Reader::new(
            count(decompressed(compression, input), scanned),
            params,
            &plan.from,
        )),
        #[cfg(feature = "parquet")]
        InputFormat::Parquet(params) => {
            // Parquet: whole-object slurp — no seek in the storage path; the
            // bound is checked request-level by the server and again here.
            // R14: the check runs WHILE reading (`take` caps at max+1) — a
            // plain `read_to_end` would slurp the whole object before the
            // bound, defense in name only (the server's 400 stays the real
            // guard, this is depth).
            let source = count(input, scanned);
            let mut buf = Vec::new();
            source.take(max_parquet_bytes + 1).read_to_end(&mut buf)?;
            if buf.len() as u64 > max_parquet_bytes {
                return Err(Error::ParquetTooLarge);
            }
            let projection = if params.projection.is_empty() {
                referenced_columns(plan)
            } else {
                params.projection
            };
            Box::new(ParquetReader::new(
                Cursor::new(buf),
                projection,
                max_parquet_bytes,
            )?)
        }
        #[cfg(not(feature = "parquet"))]
        InputFormat::Parquet(_) => {
            let _ = max_parquet_bytes;
            return Err(Error::Unsupported(
                "parquet input requires the parquet feature".into(),
            ));
        }
    };
    Ok(match scan_range {
        Some(range) => Box::new(RangeFilter::new(base, range)),
        None => base,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write as _};

    use super::*;
    use crate::{
        csv::{Header, Params},
        output::JsonOutputParams,
        sql::parse,
    };

    fn params(header: Option<Header>) -> Params {
        Params {
            header,
            ..Default::default()
        }
    }

    /// The whole event item list for `sql` over `input`.
    fn run(sql: &str, config: SelectConfig, input: &[u8]) -> Vec<Result<SelectEvent, Error>> {
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
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run(
            "SELECT * FROM S3Object",
            config,
            b"1,alice\n2,bob\n3,carol\n",
        );
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"1,alice\n2,bob\n3,carol\n".to_vec())),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 22,
                    bytes_processed: 22,
                    bytes_returned: 22,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn empty_input_stats_zeros_then_end() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, b"");
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 0,
                    bytes_processed: 0,
                    bytes_returned: 0,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn aggregate_finish_row_lands_before_stats() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run(
            "SELECT count(*) FROM S3Object s",
            config,
            b"1,alice\n2,bob\n3,carol\n",
        );
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(b"3\n".to_vec())),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 22,
                    bytes_processed: 22,
                    bytes_returned: 2,
                })),
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
            input_format: InputFormat::Csv(params(None)),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(events, vec![Err(Error::TooLarge)]);
    }

    #[test]
    fn over_1mb_output_record_errors_after_boundary_flush() {
        // The flush of the buffered small row precedes the `TooLarge` error
        // ("flush first" — review 2026-09-05 #10 ordering).
        let mut input = b"x\n".to_vec();
        input.extend_from_slice(&[b'a'; 1024 * 1024 - 2]);
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            output: OutputMode::Json(JsonOutputParams::default()),
            ..Default::default()
        };
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut it = select_iter(plan, config, Box::new(Cursor::new(input)));
        assert_eq!(
            it.next(),
            Some(Ok(SelectEvent::Records(b"{\"_1\":\"x\"}\n".to_vec())))
        );
        assert_eq!(it.next(), Some(Err(Error::TooLarge)));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn input_record_over_1mb_surfaces_reader_cap_error() {
        // The reader's own 1 MB input cap fires first — `Format`, not
        // `TooLarge` (output side). Documenting the split.
        let mut input = vec![b'a'; 1024 * 1024];
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(
            events,
            vec![Err(Error::Format("input record exceeds 1 MB".into()))]
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
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &input);
        assert_eq!(events.len(), 4);
        assert_eq!(events[0], Ok(SelectEvent::Records(row(1024 * 1024 - 2))));
        assert_eq!(events[1], Ok(SelectEvent::Records(b"x\n".to_vec())));
        assert_eq!(
            events[2],
            Ok(SelectEvent::Stats(ByteCounters {
                bytes_scanned: 1_048_577,
                bytes_processed: 1_048_577,
                bytes_returned: 1_048_577,
            }))
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
            input_format: InputFormat::Csv(params(None)),
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
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 1_800_003,
                    bytes_processed: 1_800_003,
                    bytes_returned: 1_800_003,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn cont_after_idle_emits_following_records() {
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
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
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 14,
                    bytes_processed: 14,
                    bytes_returned: 14,
                })),
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
            input_format: InputFormat::Csv(params(None)),
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
                Ok(SelectEvent::Progress(ByteCounters {
                    bytes_scanned: 1_200_003,
                    bytes_processed: 800_002,
                    bytes_returned: 800_002,
                })),
                Ok(SelectEvent::Records(two)),
                Ok(SelectEvent::Records(row(400_000))),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 1_200_003,
                    bytes_processed: 1_200_003,
                    bytes_returned: 1_200_003,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn json_lines_streams_records_stats_end() {
        let config = SelectConfig {
            input_format: InputFormat::Json(json::Params {
                ty: json::Type::Lines,
            }),
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
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 18,
                    bytes_processed: 18,
                    bytes_returned: 16,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn limit_stops_the_scan_before_late_errors() {
        // X1: LIMIT is an early stop — a record past it (here: one over the
        // 1 MB input cap) is never pulled, so the stream ends cleanly with
        // Stats+End instead of erroring on the tail.
        let mut input = b"1\n".to_vec();
        input.extend_from_slice(&[b'a'; 1024 * 1024]);
        input.push(b'\n');
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object s LIMIT 1", config, &input);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], Ok(SelectEvent::Records(b"1\n".to_vec())));
        assert!(
            matches!(&events[1], Ok(SelectEvent::Stats(_))),
            "{events:?}"
        );
        assert_eq!(events[2], Ok(SelectEvent::End));
    }

    #[test]
    fn silent_scan_emits_keepalive_cont_per_megabyte() {
        // X4: WHERE matches nothing — the stream still emits a Cont every
        // scanned MB (gated on both queues empty) so the client is never
        // silent and the producer probes the out channel mid-scan.
        let mut input = Vec::new();
        while input.len() < 1536 * 1024 {
            input.extend_from_slice(b"0\n");
        }
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object s WHERE s._1 = '1'", config, &input);
        let conts = events
            .iter()
            .filter(|e| matches!(e, Ok(SelectEvent::Cont)))
            .count();
        assert_eq!(conts, 1, "{events:?}");
        assert_eq!(events.last(), Some(&Ok(SelectEvent::End)));
    }

    #[test]
    fn silent_scan_with_progress_emits_progress_then_cont() {
        // X4 + request_progress: a silent scan emits a Progress frame (first,
        // per the ordering rule) alongside the Cont every scanned MB.
        let mut input = Vec::new();
        while input.len() < 1536 * 1024 {
            input.extend_from_slice(
                b"0
",
            );
        }
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            request_progress: true,
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object s WHERE s._1 = '1'", config, &input);
        let progress = events
            .iter()
            .filter(|e| matches!(e, Ok(SelectEvent::Progress(_))))
            .count();
        let cont = events
            .iter()
            .filter(|e| matches!(e, Ok(SelectEvent::Cont)))
            .count();
        assert_eq!(progress, 1, "{events:?}");
        assert_eq!(cont, 1, "{events:?}");
        let first_extra = events
            .iter()
            .find(|e| matches!(e, Ok(SelectEvent::Progress(_) | SelectEvent::Cont)));
        assert!(
            matches!(first_extra, Some(Ok(SelectEvent::Progress(_)))),
            "Progress must precede Cont: {events:?}"
        );
    }

    #[test]
    fn aggregate_with_limit_still_emits_one_row() {
        // Aggregate plans never consult LIMIT (the row is produced at
        // finish; a positive limit caps the single row, not the input scan).
        let config = SelectConfig {
            input_format: InputFormat::Csv(params(None)),
            ..Default::default()
        };
        let events = run(
            "SELECT count(*) FROM S3Object s LIMIT 2",
            config,
            b"1
2
3
",
        );
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(
                    b"3
"
                    .to_vec()
                )),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 6,
                    bytes_processed: 6,
                    bytes_returned: 2,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[test]
    fn bzip2_json_lines_round_trip() {
        // Compression x JSON-LINES combination: the JSON reader sits above
        // the same decoder as CSV.
        let payload = b"{\"a\": 1}
{\"b\": 2}
"
        .to_vec();
        let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::best());
        enc.write_all(&payload).unwrap();
        let bz = enc.finish().unwrap();
        let config = SelectConfig {
            input_format: InputFormat::Json(json::Params {
                ty: json::Type::Lines,
            }),
            output: OutputMode::Json(JsonOutputParams::default()),
            compression: Some(Compression::Bzip2),
            ..Default::default()
        };
        let events = run("SELECT * FROM S3Object", config, &bz);
        assert_eq!(
            events,
            vec![
                Ok(SelectEvent::Records(
                    b"{\"a\":1}
{\"b\":2}
"
                    .to_vec()
                )),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 18,
                    bytes_processed: 18,
                    bytes_returned: 16,
                })),
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
            input_format: InputFormat::Csv(params(None)),
            scan_range: Some(ScanRange {
                start: 13,
                end: Some(16),
            }),
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
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: 22,
                    bytes_processed: 6,
                    bytes_returned: 6,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_streams_records_stats_end() {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
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
                Ok(SelectEvent::Records(
                    b"{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n".to_vec()
                )),
                Ok(SelectEvent::Stats(ByteCounters {
                    bytes_scanned: buf.len() as u64,
                    bytes_processed: 8, // "alice" + "bob" row text
                    bytes_returned: 32,
                })),
                Ok(SelectEvent::End),
            ]
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_over_bound_fires_while_slurping() {
        // R14: the in-crate bound fires during the buffered read (bounded
        // memory), not after the whole object is slurped.
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice"])) as ArrayRef],
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
            max_parquet_bytes: 1,
            ..Default::default()
        };
        let events = run("SELECT name FROM S3Object s", config, &buf);
        assert_eq!(events, vec![Err(Error::ParquetTooLarge)]);
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_nested_under_csv_output_is_nested_csv_error() {
        // Decision A: a parquet list/struct cell is `Value::Json`, and CSV
        // output cannot render it — `NestedCsv` (grilling Q10). JSON-input
        // nested is the other half of the matrix (compact cell, output.rs);
        // this test pins the parquet arm through the whole event pipeline.
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, ListArray, RecordBatch, StringArray},
            buffer::OffsetBuffer,
            datatypes::{DataType, Field as ArrowField, Schema},
        };
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "tags",
            DataType::List(Arc::new(ArrowField::new("element", DataType::Utf8, true))),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ListArray::new(
                Arc::new(ArrowField::new("element", DataType::Utf8, true)),
                OffsetBuffer::new(vec![0_i32, 2, 2].into()),
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
                None,
            )) as ArrayRef],
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
            // Default output is CSV: the nested cell must error the stream,
            // never render compact JSON under CSV.
            ..Default::default()
        };
        let events = run("SELECT tags FROM S3Object s", config, &buf);
        assert_eq!(events, vec![Err(Error::NestedCsv)]);
    }
}
