//! Record readers: the `RecordReader` interface, the CSV input path, and the
//! stream stages that surround one — input `Compression` handling
//! (`decompressed`) and the `RangeFilter` scan window.
//!
//! `RecordReader` is the engine's synchronous pull contract: `next` until
//! `Ok(None)` at EOF. The reader sees the *uncompressed* stream (the
//! pipeline feeds it through `decompressed` first); record offsets reported
//! by `last_record_start` are therefore uncompressed stream bytes — the
//! basis of ScanRange (`RangeFilter`).

use std::collections::HashMap;
use std::io::Read;

use bzip2::read::MultiBzDecoder;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::MultiGzDecoder;

use crate::row::{Field, Record, Value};
use crate::SelectError;

/// One input record at a time. Implementations own their framing (CSV, JSON,
/// parquet) and their delimiters; the caller evaluates and serializes the
/// `Record`.
pub trait RecordReader {
    /// Next record, `Ok(None)` at EOF.
    fn next(&mut self) -> Result<Option<Record>, SelectError>;

    /// Uncompressed byte offset where the record most recently returned by
    /// `next` started. Default `0` — a reader without offset accounting.
    /// Purpose: ScanRange processing (design §Background) — a record counts
    /// when its first byte falls in `[start, end]`.
    fn last_record_start(&self) -> u64 {
        0
    }
}

/// Input compression (S3 Select `CompressionType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None_,
    Gzip,
    Bzip2,
}

/// Wrap the input stream in the decoder for `compression`. Multi-member
/// decoders read concatenated member streams to EOF exactly as AWS reads
/// them (review 2026-09-05b) — `gzip`/`bzip2` CLI output is multi-member;
/// a single-member decoder would silently stop at the first stream.
pub fn decompressed(compression: Compression, r: Box<dyn Read + Send>) -> Box<dyn Read + Send> {
    match compression {
        Compression::None_ => r,
        Compression::Gzip => Box::new(MultiGzDecoder::new(r)),
        Compression::Bzip2 => Box::new(MultiBzDecoder::new(r)),
    }
}

/// CSV input options (S3 Select `InputSerialization.CSV`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvParams {
    pub field_delimiter: u8,
    pub record_delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub comments: Option<u8>,
    pub header: CsvHeader,
    /// Known deviation (review 2026-09-05 #7): the `csv` crate cannot
    /// distinguish a record delimiter inside a quoted field, so this flag is
    /// not enforced — `true` and `false` both parse permissively (AWS
    /// `true`). Documented here and pinned by tests, never silently ignored.
    pub allow_quoted_record_delimiter: bool,
}

/// First-line handling for CSV input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvHeader {
    /// First non-comment line names the columns; data rows carry those names.
    Use,
    /// First non-comment line is dropped; columns are `_1.._n`.
    Ignore,
    /// No header handling; every line is data, columns `_1.._n` per row width.
    None_,
}

/// `csv` crate under the `RecordReader` interface.
pub struct CsvReader<R: Read> {
    inner: csv::Reader<R>,
    mode: CsvHeader,
    /// USE header names (row 0), original case.
    names: Vec<String>,
    /// USE/IGNORE: the header line has been consumed.
    header_done: bool,
    last_record_start: u64,
}

/// AWS input-record cap (design §1, review 2026-09-05 #3): a record over
/// 1 MB errors the stream out — bounds memory, defuses decompression bombs.
/// Measured on the reader's own span accounting: the record's first byte
/// through its terminator (a span that may also include comment lines
/// immediately preceding the record, which the `csv` parser consumes inline).
const MAX_RECORD: u64 = 1024 * 1024;
const CAP_MESSAGE: &str = "input record exceeds 1 MB";

impl<R: Read> CsvReader<R> {
    pub fn new(reader: R, params: CsvParams) -> Self {
        let mut b = ReaderBuilder::new();
        b.delimiter(params.field_delimiter)
            .terminator(csv::Terminator::Any(params.record_delimiter))
            .quote(params.quote)
            .escape(Some(params.escape))
            .has_headers(false)
            .flexible(true);
        if let Some(c) = params.comments {
            b.comment(Some(c));
        }
        Self {
            inner: b.from_reader(reader),
            mode: params.header,
            names: Vec::new(),
            header_done: false,
            last_record_start: 0,
        }
    }

    /// One framed record, the cap applied. `None` = EOF clean.
    fn read_record(&mut self) -> Result<Option<StringRecord>, SelectError> {
        let mut record = StringRecord::new();
        if !self.inner.read_record(&mut record).map_err(from_csv)? {
            return Ok(None);
        }
        // `csv` positions are logical-stream offsets (seek-compatible), so
        // the end - start span is exact regardless of internal buffer refills.
        let end = self.inner.position().byte();
        if end - record_start(&record) > MAX_RECORD {
            return Err(SelectError::Format(CAP_MESSAGE.into()));
        }
        Ok(Some(record))
    }
}

/// `csv` error mapping: I/O passes through (`SelectError::Io`); parse-level
/// failures (UTF-8, formatting) surface as in-stream `Format` (design §3).
fn from_csv(e: csv::Error) -> SelectError {
    let msg = format!("csv input: {e}");
    match e.into_kind() {
        csv::ErrorKind::Io(io) => SelectError::Io(io),
        _ => SelectError::Format(msg),
    }
}

/// First byte offset of `record` — `csv` sets a position on every record it
/// returns (its own doc guarantee).
fn record_start(record: &StringRecord) -> u64 {
    record
        .position()
        .expect("csv sets a position on records it reads")
        .byte()
}

/// `_1.._n` for a row of `len` fields — the positional alias set.
fn default_names(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("_{i}")).collect()
}

/// Case-insensitive column lookup: every key lowercased (S3 Select unquoted
/// identifiers are case-insensitive). Duplicates collapse, later wins — the
/// engine notes ambiguity from the loss of cardinality at lookup time.
pub fn name_index(names: &[String]) -> HashMap<String, usize> {
    names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.to_lowercase(), i))
        .collect()
}

impl<R: Read> RecordReader for CsvReader<R> {
    fn next(&mut self) -> Result<Option<Record>, SelectError> {
        if !self.header_done {
            self.header_done = true;
            match self.mode {
                CsvHeader::Use => match self.read_record()? {
                    None => return Ok(None),
                    Some(header) => {
                        self.names = header.iter().map(|f| f.to_string()).collect();
                    }
                },
                CsvHeader::Ignore => {
                    if self.read_record()?.is_none() {
                        return Ok(None);
                    }
                }
                CsvHeader::None_ => {}
            }
        }
        let record = match self.read_record()? {
            None => return Ok(None),
            Some(record) => record,
        };
        self.last_record_start = record_start(&record);
        let names = match self.mode {
            CsvHeader::Use => self.names.clone(),
            _ => default_names(record.len()),
        };
        let fields = record
            .iter()
            .map(|f| Field::Present(Value::String(f.to_string())))
            .collect();
        Ok(Some(Record::Csv(fields, names)))
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}

/// ScanRange window over a record reader.
///
/// Offsets are *uncompressed* bytes — the filter wraps the format reader,
/// not the raw stream, so a record counts when its first byte falls in
/// `[start, end]` with `end = None` meaning "no upper bound" (the stream's
/// own EOF). Pre-range records are consumed and dropped; the first record
/// whose start passes `end` stops the stream (`Ok(None)`), and nothing past
/// it is ever read. JSON DOCUMENT's whole object is one byte-0 record, so
/// `start > 0` yields nothing — correct, not a bug (spec §Background).
pub struct RangeFilter<R: RecordReader> {
    inner: R,
    start: u64,
    end: Option<u64>,
    /// A record whose start passed `end` stopped the stream.
    stopped: bool,
}

impl<R: RecordReader> RangeFilter<R> {
    /// The (start, end) pair is the *resolved* window — the adapter computes
    /// it (Task 12): both bounds pass through; end-only resolves `start =
    /// size - end`. The server validates the window (Task 13), so nothing
    /// checks bounds here.
    pub fn new(inner: R, start: u64, end: Option<u64>) -> Self {
        Self {
            inner,
            start,
            end,
            stopped: false,
        }
    }
}

impl<R: RecordReader> RecordReader for RangeFilter<R> {
    fn next(&mut self) -> Result<Option<Record>, SelectError> {
        if self.stopped {
            return Ok(None);
        }
        loop {
            let record = match self.inner.next()? {
                None => return Ok(None),
                Some(record) => record,
            };
            let at = self.inner.last_record_start();
            if at < self.start {
                continue;
            }
            if let Some(end) = self.end
                && at > end
            {
                self.stopped = true;
                return Ok(None);
            }
            return Ok(Some(record));
        }
    }

    fn last_record_start(&self) -> u64 {
        self.inner.last_record_start()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use bzip2::write::BzEncoder;
    use flate2::write::GzEncoder;

    use crate::json::{JsonReader, JsonType};
    use crate::sql::parse;

    use super::*;

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

    fn csv(fields: &[&str], names: &[&str]) -> Record {
        Record::Csv(
            fields
                .iter()
                .map(|f| Field::Present(Value::String((*f).to_string())))
                .collect(),
            names.iter().map(|n| (*n).to_string()).collect(),
        )
    }

    #[test]
    fn use_header_names() {
        let mut r = CsvReader::new(
            Cursor::new(b"id,name\n1,alice\n2,bob\n"),
            params(CsvHeader::Use),
        );
        assert_eq!(r.next().unwrap().unwrap(), csv(&["1", "alice"], &["id", "name"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["2", "bob"], &["id", "name"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn ignore_first_line() {
        let mut r = CsvReader::new(
            Cursor::new(b"id,name\n1,alice\n"),
            params(CsvHeader::Ignore),
        );
        assert_eq!(r.next().unwrap().unwrap(), csv(&["1", "alice"], &["_1", "_2"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn none_does_not_skip() {
        let mut r = CsvReader::new(Cursor::new(b"id,name\n"), params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["id", "name"], &["_1", "_2"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn comments_skipped() {
        let p = CsvParams {
            comments: Some(b'#'),
            ..params(CsvHeader::Use)
        };
        let mut r = CsvReader::new(Cursor::new(b"# generated file\nid,name\n1,alice\n"), p);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["1", "alice"], &["id", "name"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn custom_delimiter() {
        let p = CsvParams {
            field_delimiter: b'|',
            ..params(CsvHeader::Use)
        };
        let mut r = CsvReader::new(Cursor::new(b"id|name\n1|alice\n"), p);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["1", "alice"], &["id", "name"]));
    }

    #[test]
    fn quoted_record_delimiter_permissive_for_both_flags() {
        // Known deviation (review 2026-09-05 #7): quoted record delimiters
        // are not distinguishable by the csv crate — `\n` inside quotes is
        // data, one record comes out, and `allow_quoted_record_delimiter`
        // has no effect: both `true` and `false` parse permissively (AWS
        // `true`). Never silently ignored.
        for flag in [true, false] {
            let p = CsvParams {
                allow_quoted_record_delimiter: flag,
                ..params(CsvHeader::None_)
            };
            let mut r = CsvReader::new(Cursor::new(b"\"a\nb\",c\n"), p);
            assert_eq!(
                r.next().unwrap().unwrap(),
                csv(&["a\nb", "c"], &["_1", "_2"])
            );
            assert_eq!(r.next().unwrap(), None);
        }
    }

    #[test]
    fn ragged_tail_row_width() {
        let mut r = CsvReader::new(Cursor::new(b"a,b\n1\n"), params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["a", "b"], &["_1", "_2"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["1"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn ragged_row_keeps_use_header_width() {
        // USE names stay as declared; a short row keeps them (the engine
        // sees MISSING past the row's fields).
        let mut r = CsvReader::new(Cursor::new(b"id,name\n1\n"), params(CsvHeader::Use));
        let row = r.next().unwrap().unwrap();
        assert_eq!(row, csv(&["1"], &["id", "name"]));
    }

    #[test]
    fn input_record_cap_message() {
        let mut input = vec![b'a'; 1024 * 1024 + 1];
        input.push(b'\n');
        let mut r = CsvReader::new(Cursor::new(input), params(CsvHeader::None_));
        match r.next() {
            Err(SelectError::Format(msg)) => {
                assert_eq!(msg, "input record exceeds 1 MB");
                // Display matches error.rs's Format framing verbatim.
                assert_eq!(
                    SelectError::Format(msg).to_string(),
                    "S3 select: input error: input record exceeds 1 MB"
                );
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn input_record_cap_exact_limit_allowed() {
        // Span (content + record terminator) exactly 1 MB: past it errors,
        // at it passes.
        let mut input = vec![b'a'; 1024 * 1024 - 1];
        input.push(b'\n');
        let mut r = CsvReader::new(Cursor::new(input), params(CsvHeader::None_));
        assert_eq!(
            r.next().unwrap().unwrap(),
            Record::Csv(
                vec![Field::Present(Value::String("a".repeat(1024 * 1024 - 1)))],
                vec!["_1".to_string()],
            )
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn last_record_start_tracks_records() {
        let mut r = CsvReader::new(Cursor::new(b"aaa\nbb\nc\n"), params(CsvHeader::None_));
        assert_eq!(r.last_record_start(), 0);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 4);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 7);
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.last_record_start(), 7);
    }

    #[test]
    fn last_record_start_across_buffer_refills() {
        // First row exceeds the default 8 KiB read buffer: offsets must stay
        // logical-stream aligned despite read-ahead.
        let mut input = vec![b'a'; 16 * 1024];
        input.push(b'\n');
        input.extend_from_slice(b"x,y\n");
        let mut r = CsvReader::new(Cursor::new(input), params(CsvHeader::None_));
        assert!(r.next().unwrap().is_some());
        assert_eq!(
            r.next().unwrap().unwrap(),
            csv(&["x", "y"], &["_1", "_2"])
        );
        assert_eq!(r.last_record_start(), 16 * 1024 + 1);
    }

    #[test]
    fn name_index_lowercases_keys() {
        let idx = name_index(&["ID".to_string(), "Name".to_string(), "name".to_string()]);
        assert_eq!(idx.get("id"), Some(&0));
        // Lookups lowercase the identifier too (unquoted = case-insensitive).
        assert_eq!(idx.get(&"NAME".to_lowercase()), Some(&2)); // duplicates: later wins
        assert_eq!(idx.get("iD"), None); // keys are lowercased
        assert_eq!(name_index(&[]).len(), 0);
    }

    // ------------------------------------------------------------------
    // Decompression + scan range (Task 10).
    // ------------------------------------------------------------------

    /// Four records, starts at 0, 13, 16, 19; 22 bytes total. A window
    /// opening at byte 10 lands inside record 1's span (bytes 0..13) — its
    /// partial remainder must not be yielded: a record counts by its first
    /// byte, never by overlap.
    const FOUR: &[u8] = b"0123456789ab\nr1\nr2\nr3\n";

    fn csv_reader(bytes: &[u8]) -> CsvReader<Cursor<&[u8]>> {
        CsvReader::new(Cursor::new(bytes), params(CsvHeader::None_))
    }

    /// One `next` call observable through `RangeFilter`: counts inner reads.
    struct Counting<R: RecordReader>(R, usize);

    impl<R: RecordReader> RecordReader for Counting<R> {
        fn next(&mut self) -> Result<Option<Record>, SelectError> {
            self.1 += 1;
            self.0.next()
        }

        fn last_record_start(&self) -> u64 {
            self.0.last_record_start()
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    fn bzip2(bytes: &[u8]) -> Vec<u8> {
        let mut enc = BzEncoder::new(Vec::new(), bzip2::Compression::best());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn range_start_inside_span_drops_partial_record() {
        // start 10 falls inside record 1's span (0..13): record 1 drops
        // (start 0 < 10 — consumed, not emitted), the rest are in range.
        let mut r = RangeFilter::new(csv_reader(FOUR), 10, None);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.last_record_start(), 13);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r3"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn range_both_bounds_inclusive() {
        let mut r = RangeFilter::new(csv_reader(FOUR), 13, Some(16));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"])); // 13
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"])); // 16 == end
        assert_eq!(r.next().unwrap(), None); // 19 > 16 stops the stream
    }

    #[test]
    fn range_stop_not_consuming_past_end() {
        // A record whose start passes `end` stops the stream: 4 inner reads
        // (1 dropped + 2 yielded + 1 stop) and none past the stop on a
        // further pull.
        let mut r = RangeFilter::new(Counting(csv_reader(FOUR), 0), 13, Some(16));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 4);
        assert!(r.next().unwrap().is_none());
        assert_eq!(r.inner.1, 4);
    }

    #[test]
    fn range_empty_window_stops_immediately() {
        // No record starts at 6: record 1 drops (start 0 < 6), record 2's
        // start 13 > end 6 stops — `Ok(None)` after only two inner reads.
        let mut r = RangeFilter::new(Counting(csv_reader(FOUR), 0), 6, Some(6));
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 2);
    }

    #[test]
    fn range_beyond_eof_drains_and_returns_none() {
        // Window past the last record: all four are pre-range, consumed and
        // dropped; the fifth inner read is the EOF probe that answers `None`.
        let mut r = RangeFilter::new(Counting(csv_reader(FOUR), 0), 30, None);
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 5);
    }

    #[test]
    fn range_end_only_resolved_via_size() {
        // End-only means "last N bytes" → start = size - N (the server
        // resolves it, T13); the window tops at the object's last byte, so
        // the upper bound is the stream's own EOF. Last 9 of 22 bytes.
        let size: u64 = 22;
        let start = size - 9;
        assert_eq!(start, 13);
        let mut r = RangeFilter::new(csv_reader(FOUR), start, None);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r3"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_start_above_zero_yields_nothing() {
        // DOCUMENT: the whole object is one byte-0 record, so start > 0
        // yields zero records — documented semantics, not a bug (spec).
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let reader = JsonReader::new(
            Cursor::new(b"{\"a\": 1}\n".to_vec()),
            JsonType::Document,
            &plan.from,
        );
        let mut r = RangeFilter::new(reader, 1, None);
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn none_compression_passes_through() {
        let reader = decompressed(Compression::None_, Box::new(Cursor::new(b"a\nb\n")));
        let mut r = CsvReader::new(reader, params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["a"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["b"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn gzip_round_trip() {
        let reader =
            decompressed(Compression::Gzip, Box::new(Cursor::new(gzip(b"aaa\nbb\nc\n"))));
        let mut r = CsvReader::new(reader, params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn gzip_multi_member_reads_all_streams() {
        // Concatenated members — exactly what the gzip CLI emits; the
        // single-member decoder would stop after the first stream.
        let mut both = gzip(b"aaa\n");
        both.extend(gzip(b"bb\nc\n"));
        let reader = decompressed(Compression::Gzip, Box::new(Cursor::new(both)));
        let mut r = CsvReader::new(reader, params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn bzip2_round_trip() {
        let reader =
            decompressed(Compression::Bzip2, Box::new(Cursor::new(bzip2(b"aaa\nbb\nc\n"))));
        let mut r = CsvReader::new(reader, params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn bzip2_multi_member_reads_all_streams() {
        // Two encoder streams concatenated — the multi-member decoder (R1:
        // spec wins over the brief's single-stream reader) must read both.
        let mut both = bzip2(b"aaa\n");
        both.extend(bzip2(b"bb\nc\n"));
        let reader = decompressed(Compression::Bzip2, Box::new(Cursor::new(both)));
        let mut r = CsvReader::new(reader, params(CsvHeader::None_));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }
}
