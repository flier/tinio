//! CSV input path: S3 Select `InputSerialization.CSV` framing over the
//! `csv` crate (imported as `_csv`), under [`RecordReader`].

use std::{
    cell::RefCell,
    io,
    io::{ErrorKind, Read},
    rc::Rc,
};

use _csv::{ReaderBuilder, StringRecord};
use parse_display::{Display, FromStr};
use smart_default::SmartDefault;

use crate::{
    error::Error,
    record::{CAP_MESSAGE, MAX_RECORD, RecordReader},
    row::{Columns, Field, Record, Value},
};

/// CSV input options (S3 Select `InputSerialization.CSV`).
///
/// AWS defaults: `,`, `\n`, `"`, `"`, no comments, `NONE` header, quoted
/// record delimiters allowed.
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault)]
pub struct Params {
    #[default(b',')]
    pub field_delimiter: u8,
    #[default(b'\n')]
    pub record_delimiter: u8,
    #[default(b'"')]
    pub quote: u8,
    #[default(b'"')]
    pub escape: u8,
    pub comments: Option<u8>,
    pub header: Option<Header>,
    /// Known deviation (review 2026-09-05 #7): the `csv` crate cannot
    /// distinguish a record delimiter inside a quoted field, so this flag is
    /// not enforced — `true` and `false` both parse permissively (AWS
    /// `true`). Documented here and pinned by tests, never silently ignored.
    #[default = true]
    pub allow_quoted_record_delimiter: bool,
}

/// First-line handling for CSV input (S3 Select `FileHeaderInfo` minus `NONE`).
///
/// Absence of header handling is `Option::None` — wire `NONE` (and an unset
/// field) map there, never to a variant.
///
/// # Examples
///
/// ```
/// use tinio_select::csv::Header;
///
/// assert_eq!(Header::Use.to_string(), "USE");
/// assert_eq!("IGNORE".parse(), Ok(Header::Ignore));
/// assert!("NONE".parse::<Header>().is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, FromStr)]
#[display(style = "UPPERCASE")]
pub enum Header {
    /// First non-comment line names the columns; data rows carry those names.
    Use,
    /// First non-comment line is dropped; columns are `_1.._n`.
    Ignore,
}

/// Cap state shared with the counting wrapper: reset per record; `capped`
/// marks the wrapper tripping the cap mid-record (the csv layer surfaces an
/// io error, the reader maps it to the own Format message).
#[derive(Default)]
struct CapState {
    count: u64,
    capped: bool,
}

/// Read wrapper directly below the `csv` reader (review 2026-09-06b R5):
/// the crate's own record-span check is post-hoc — `csv` buffers a whole
/// record before returning it, so a compressed input expanding into one
/// giant field would buffer unboundedly before that check fires (the
/// "defuses decompression bombs" claim did not hold for CSV; JSON checks
/// incrementally and is safe). The wrapper counts bytes released to the
/// reader since the last reset and errors past `MAX_RECORD` + the read-ahead
/// allowance — the `csv` crate's internal buffer (8 KiB) prefetches across
/// record boundaries, so the exact per-record decision stays the span check
/// (same message) while the wrapper bounds the memory envelope.
struct CappedRead<R: Read> {
    inner: R,
    state: Rc<RefCell<CapState>>,
}

/// Read-ahead allowance over the span cap for the wrapper: the internal
/// buffer can prefetch the first bytes of the next record during a record's
/// own fill, so a just-under-cap record would otherwise false-trigger.
pub(crate) const CAP_READ_AHEAD: u64 = 64 * 1024;

fn cap_with_allowance() -> u64 {
    MAX_RECORD + CAP_READ_AHEAD
}

impl<R: Read> CappedRead<R> {
    fn new(inner: R, state: Rc<RefCell<CapState>>) -> Self {
        Self { inner, state }
    }
}

impl<R: Read> Read for CappedRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        let mut state = self.state.borrow_mut();
        state.count += n as u64;
        if state.count > cap_with_allowance() {
            state.capped = true;
            return Err(io::Error::new(ErrorKind::InvalidData, CAP_MESSAGE));
        }
        Ok(n)
    }
}

/// `csv` crate under the [`RecordReader`] interface.
pub struct Reader<R: Read> {
    inner: _csv::Reader<CappedRead<R>>,
    cap: Rc<RefCell<CapState>>,
    /// The framed-record scratch buffer, reused across calls (`csv` keeps
    /// its capacity — a per-call `StringRecord::new()` would re-grow for
    /// every record; review 2026-09-06 simplify).
    buf: StringRecord,
    mode: Option<Header>,
    /// USE header names (row 0), original case — `Rc`-shared into every
    /// record (review S7) instead of cloned per record (X8).
    names_header: Rc<Vec<String>>,
    /// The last record's positional name set, cached by width (ragged rows
    /// rebuild; a constant width reuses one `Rc`).
    positional: (usize, Rc<Vec<String>>),
    /// USE/IGNORE: the header line has been consumed.
    header_done: bool,
    last_record_start: u64,
}

impl<R: Read> Reader<R> {
    pub fn new(reader: R, params: Params) -> Self {
        let mut b = ReaderBuilder::new();
        b.delimiter(params.field_delimiter)
            .terminator(_csv::Terminator::Any(params.record_delimiter))
            .quote(params.quote)
            .escape(Some(params.escape))
            .has_headers(false)
            .flexible(true);
        if let Some(c) = params.comments {
            b.comment(Some(c));
        }
        let cap = Rc::new(RefCell::new(CapState::default()));
        Self {
            inner: b.from_reader(CappedRead::new(reader, cap.clone())),
            cap,
            buf: StringRecord::new(),
            mode: params.header,
            names_header: Rc::new(Vec::new()),
            positional: (0, Rc::new(Vec::new())),
            header_done: false,
            last_record_start: 0,
        }
    }

    /// One framed record into the reused `self.buf`, the cap applied.
    /// `true` = a record was read; `false` = clean EOF.
    fn read_record(&mut self) -> Result<bool, Error> {
        // Reset the wrapper's window: a record's bytes count from here; the
        // span check below stays the exact per-record authority.
        let mut state = self.cap.borrow_mut();
        state.count = 0;
        state.capped = false;
        drop(state);
        let ok = self.inner.read_record(&mut self.buf).map_err(|e| {
            if self.cap.borrow().capped {
                Error::Format(CAP_MESSAGE.into())
            } else {
                map_error(e, "input")
            }
        })?;
        if !ok {
            return Ok(false);
        }
        // `csv` positions are logical-stream offsets (seek-compatible), so
        // the end - start span is exact regardless of internal buffer refills.
        let end = self.inner.position().byte();
        if end - record_start(&self.buf) > MAX_RECORD {
            return Err(Error::Format(CAP_MESSAGE.into()));
        }
        Ok(true)
    }
}

/// `csv` crate error → `Error`: I/O passes through (`Io`), the rest is a
/// `Format` failure. Shared by the input reader (context "input") and the
/// output writer (context "output") — practically unreachable for `&str`
/// fields into a `Vec`.
pub(crate) fn map_error(e: _csv::Error, context: &str) -> Error {
    let msg = format!("csv {context}: {e}");
    match e.into_kind() {
        _csv::ErrorKind::Io(io) => Error::Io(io.into()),
        _ => Error::Format(msg),
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
fn positional_names(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("_{i}")).collect()
}

impl<R: Read> RecordReader for Reader<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        if !self.header_done {
            self.header_done = true;
            match self.mode {
                Some(Header::Use) | Some(Header::Ignore) => {
                    if !self.read_record()? {
                        return Ok(None);
                    }
                    if self.mode == Some(Header::Use) {
                        self.names_header =
                            Rc::new(self.buf.iter().map(|f| f.to_string()).collect());
                    }
                }
                None => {}
            }
        }
        if !self.read_record()? {
            return Ok(None);
        }
        self.last_record_start = record_start(&self.buf);
        let names = match self.mode {
            Some(Header::Use) => self.names_header.clone(),
            _ => {
                let width = self.buf.len();
                if width != self.positional.0 {
                    self.positional = (width, Rc::new(positional_names(width)));
                }
                self.positional.1.clone()
            }
        };
        let fields = self
            .buf
            .iter()
            .map(|f| Field::Present(Value::String(f.to_string())))
            .collect();
        Ok(Some(Record::Csv(Columns::new(fields, names))))
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{
        error::Error,
        record::{Compression, decompressed},
        row::{Columns, Field, Record, Value},
    };

    fn params(header: Option<Header>) -> Params {
        Params {
            header,
            ..Default::default()
        }
    }

    fn row(fields: &[&str], names: &[&str]) -> Record {
        Record::Csv(Columns::from_names(
            fields
                .iter()
                .map(|f| Field::Present(Value::String((*f).to_string())))
                .collect(),
            names.iter().map(|n| (*n).to_string()).collect(),
        ))
    }

    #[test]
    fn use_header_names() {
        let mut r = Reader::new(
            Cursor::new(b"id,name\n1,alice\n2,bob\n"),
            params(Some(Header::Use)),
        );
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["1", "alice"], &["id", "name"])
        );
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["2", "bob"], &["id", "name"])
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn ignore_first_line() {
        let mut r = Reader::new(
            Cursor::new(b"id,name\n1,alice\n"),
            params(Some(Header::Ignore)),
        );
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["1", "alice"], &["_1", "_2"])
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn none_does_not_skip() {
        let mut r = Reader::new(Cursor::new(b"id,name\n"), params(None));
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["id", "name"], &["_1", "_2"])
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn comments_skipped() {
        let p = Params {
            comments: Some(b'#'),
            ..params(Some(Header::Use))
        };
        let mut r = Reader::new(Cursor::new(b"# generated file\nid,name\n1,alice\n"), p);
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["1", "alice"], &["id", "name"])
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn custom_delimiter() {
        let p = Params {
            field_delimiter: b'|',
            ..params(Some(Header::Use))
        };
        let mut r = Reader::new(Cursor::new(b"id|name\n1|alice\n"), p);
        assert_eq!(
            r.next().unwrap().unwrap(),
            row(&["1", "alice"], &["id", "name"])
        );
    }

    #[test]
    fn quoted_record_delimiter_permissive_for_both_flags() {
        // Known deviation (review 2026-09-05 #7): quoted record delimiters
        // are not distinguishable by the csv crate — `\n` inside quotes is
        // data, one record comes out, and `allow_quoted_record_delimiter`
        // has no effect: both `true` and `false` parse permissively (AWS
        // `true`). Never silently ignored.
        for flag in [true, false] {
            let p = Params {
                allow_quoted_record_delimiter: flag,
                ..params(None)
            };
            let mut r = Reader::new(Cursor::new(b"\"a\nb\",c\n"), p);
            assert_eq!(
                r.next().unwrap().unwrap(),
                row(&["a\nb", "c"], &["_1", "_2"])
            );
            assert_eq!(r.next().unwrap(), None);
        }
    }

    #[test]
    fn ragged_tail_row_width() {
        let mut r = Reader::new(Cursor::new(b"a,b\n1\n"), params(None));
        assert_eq!(r.next().unwrap().unwrap(), row(&["a", "b"], &["_1", "_2"]));
        assert_eq!(r.next().unwrap().unwrap(), row(&["1"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn ragged_row_keeps_use_header_width() {
        // USE names stay as declared; a short row keeps them (the engine
        // sees MISSING past the row's fields).
        let mut r = Reader::new(Cursor::new(b"id,name\n1\n"), params(Some(Header::Use)));
        let got = r.next().unwrap().unwrap();
        assert_eq!(got, row(&["1"], &["id", "name"]));
    }

    #[test]
    fn input_record_cap_message() {
        let mut input = vec![b'a'; 1024 * 1024 + 1];
        input.push(b'\n');
        let mut r = Reader::new(Cursor::new(input), params(None));
        match r.next() {
            Err(Error::Format(msg)) => {
                assert_eq!(msg, "input record exceeds 1 MB");
                assert_eq!(
                    Error::Format(msg).to_string(),
                    "S3 select: input error: input record exceeds 1 MB"
                );
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn input_record_cap_exact_limit_allowed() {
        let mut input = vec![b'a'; 1024 * 1024 - 1];
        input.push(b'\n');
        let mut r = Reader::new(Cursor::new(input), params(None));
        assert_eq!(
            r.next().unwrap().unwrap(),
            Record::Csv(Columns::from_names(
                vec![Field::Present(Value::String("a".repeat(1024 * 1024 - 1)))],
                vec!["_1".to_string()],
            ))
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn incremental_cap_bounds_compressed_bomb() {
        use std::io::Write;

        use flate2::write::GzEncoder;

        let mut bomb = vec![b'a'; 2 * 1024 * 1024];
        bomb.push(b'\n');
        let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&bomb).unwrap();
        let gzipped = enc.finish().unwrap();
        let reader = decompressed(Some(Compression::Gzip), Box::new(Cursor::new(gzipped)));
        let mut r = Reader::new(reader, params(None));
        match r.next() {
            Err(Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn last_record_start_tracks_records() {
        let mut r = Reader::new(Cursor::new(b"aaa\nbb\nc\n"), params(None));
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
        let mut input = vec![b'a'; 16 * 1024];
        input.push(b'\n');
        input.extend_from_slice(b"x,y\n");
        let mut r = Reader::new(Cursor::new(input), params(None));
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.next().unwrap().unwrap(), row(&["x", "y"], &["_1", "_2"]));
        assert_eq!(r.last_record_start(), 16 * 1024 + 1);
    }
}
