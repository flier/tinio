//! CSV record reader: the `RecordReader` interface plus the CSV input path.
//!
//! `RecordReader` is the engine's synchronous pull contract: `next` until
//! `Ok(None)` at EOF. Input is the *uncompressed* stream (decompression
//! happens outside, in the pipeline); record offsets reported by
//! `last_record_start` are thus uncompressed stream bytes.

use std::collections::HashMap;
use std::io::Read;

use csv::{ReaderBuilder, StringRecord};

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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

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
}
