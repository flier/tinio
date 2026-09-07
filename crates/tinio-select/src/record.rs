//! Record readers: the `RecordReader` interface and the stream stages that
//! surround one — input `Compression` handling (`decompressed`) and the
//! `RangeFilter` scan window. Format readers live beside this module
//! ([`crate::csv`], [`crate::json`], …).
//!
//! `RecordReader` is the engine's synchronous pull contract: `next` until
//! `Ok(None)` at EOF. The reader sees the *uncompressed* stream (the
//! pipeline feeds it through `decompressed` first); record offsets reported
//! by `last_record_start` are therefore uncompressed stream bytes — the
//! basis of ScanRange (`RangeFilter`).

use std::io::Read;

use bzip2::read::MultiBzDecoder;
use flate2::read::MultiGzDecoder;
use parse_display::{Display, FromStr, ParseError};

use crate::{error::Error, row::Record};

/// One input record at a time. Implementations own their framing (CSV, JSON,
/// parquet) and their delimiters; the caller evaluates and serializes the
/// `Record`.
pub trait RecordReader {
    /// Next record, `Ok(None)` at EOF.
    fn next(&mut self) -> Result<Option<Record>, Error>;

    /// Uncompressed byte offset where the record most recently returned by
    /// `next` started. Default `0` — a reader without offset accounting.
    /// Purpose: ScanRange processing (design §Background) — a record counts
    /// when its first byte falls in `[start, end]`.
    fn last_record_start(&self) -> u64 {
        0
    }
}

/// Boxing erasure: the events adapter composes per-format readers behind
/// `Box<dyn RecordReader>`.
impl RecordReader for Box<dyn RecordReader> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        (**self).next()
    }

    fn last_record_start(&self) -> u64 {
        (**self).last_record_start()
    }
}

/// Input compression codecs (S3 Select `CompressionType` minus `NONE`).
///
/// Absence of compression is `Option::None` at the call site — the wire
/// value `NONE` (and an unset field) map there, never to a variant.
///
/// # Examples
///
/// ```
/// use tinio_select::record::Compression;
///
/// assert_eq!(Compression::Gzip.to_string(), "GZIP");
/// assert_eq!(
///     Compression::from_wire("GZIP").unwrap(),
///     Some(Compression::Gzip)
/// );
/// assert_eq!(Compression::from_wire("NONE").unwrap(), None);
/// assert!(Compression::from_wire("LZO").is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, FromStr)]
#[display(style = "UPPERCASE")]
pub enum Compression {
    Gzip,
    Bzip2,
}

impl Compression {
    /// S3 Select `CompressionType` wire: `NONE` → `Ok(None)`; `GZIP`/`BZIP2`
    /// → `Ok(Some)`; anything else → err (never silently treated as none).
    pub fn from_wire(value: &str) -> Result<Option<Self>, ParseError> {
        if value == "NONE" {
            Ok(None)
        } else {
            Ok(Some(value.parse()?))
        }
    }
}

/// Wrap the input stream in the decoder for `compression`. Multi-member
/// decoders read concatenated member streams to EOF exactly as AWS reads
/// them (review 2026-09-05b) — `gzip`/`bzip2` CLI output is multi-member;
/// a single-member decoder would silently stop at the first stream.
pub fn decompressed(
    compression: Option<Compression>,
    r: Box<dyn Read + Send>,
) -> Box<dyn Read + Send> {
    match compression {
        None => r,
        Some(Compression::Gzip) => Box::new(MultiGzDecoder::new(r)),
        Some(Compression::Bzip2) => Box::new(MultiBzDecoder::new(r)),
    }
}

/// AWS input-record cap (design §1, review 2026-09-05 #3): a record over
/// 1 MB errors the stream out — bounds memory, defuses decompression bombs.
/// Measured on the reader's own span accounting: the record's first byte
/// through its terminator (a span that may also include comment lines
/// immediately preceding the record, which the `csv` parser consumes inline).
pub(crate) const MAX_RECORD: u64 = 1024 * 1024;
pub(crate) const CAP_MESSAGE: &str = "input record exceeds 1 MB";

/// A resolved ScanRange window: a record counts when its first byte falls
/// in `[start, end]`; `end = None` = no upper bound (the stream's own EOF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanRange {
    pub start: u64,
    pub end: Option<u64>,
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
    pub fn new(inner: R, range: ScanRange) -> Self {
        Self {
            inner,
            start: range.start,
            end: range.end,
            stopped: false,
        }
    }
}

impl<R: RecordReader> RecordReader for RangeFilter<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
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

    use super::*;
    use crate::{
        csv::{self, Header, Params},
        json,
        row::{Columns, Field, Value},
        sql::parse,
    };

    fn params(header: Option<Header>) -> Params {
        Params {
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
        Record::Csv(Columns::from_names(
            fields
                .iter()
                .map(|f| Field::Present(Value::String((*f).to_string())))
                .collect(),
            names.iter().map(|n| (*n).to_string()).collect(),
        ))
    }

    fn csv_reader(bytes: &[u8]) -> csv::Reader<Cursor<&[u8]>> {
        csv::Reader::new(Cursor::new(bytes), params(None))
    }

    /// One `next` call observable through `RangeFilter`: counts inner reads.
    struct Counting<R: RecordReader>(R, usize);

    impl<R: RecordReader> RecordReader for Counting<R> {
        fn next(&mut self) -> Result<Option<Record>, Error> {
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

    /// Four records, starts at 0, 13, 16, 19; 22 bytes total. A window
    /// opening at byte 10 lands inside record 1's span (bytes 0..13) — its
    /// partial remainder must not be yielded: a record counts by its first
    /// byte, never by overlap.
    const FOUR: &[u8] = b"0123456789ab\nr1\nr2\nr3\n";

    #[test]
    fn compression_from_wire() {
        // S3 Select CompressionType wire mapping: `NONE` (and an unset
        // field) map to `None`, the two codecs map to `Some`, anything
        // else is an error — never silently treated as uncompressed.
        assert_eq!(Compression::from_wire("NONE").unwrap(), None);
        assert_eq!(
            Compression::from_wire("GZIP").unwrap(),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::from_wire("BZIP2").unwrap(),
            Some(Compression::Bzip2)
        );
        // The FromStr derive is UPPERCASE-styled: lowercase and unknown
        // codecs are rejections.
        assert!(Compression::from_wire("gzip").is_err());
        assert!(Compression::from_wire("LZO").is_err());
    }

    #[test]
    fn range_start_inside_span_drops_partial_record() {
        let mut r = RangeFilter::new(
            csv_reader(FOUR),
            ScanRange {
                start: 10,
                end: None,
            },
        );
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.last_record_start(), 13);
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r3"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn range_both_bounds_inclusive() {
        let mut r = RangeFilter::new(
            csv_reader(FOUR),
            ScanRange {
                start: 13,
                end: Some(16),
            },
        );
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn range_stop_not_consuming_past_end() {
        let mut r = RangeFilter::new(
            Counting(csv_reader(FOUR), 0),
            ScanRange {
                start: 13,
                end: Some(16),
            },
        );
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 4);
        assert!(r.next().unwrap().is_none());
        assert_eq!(r.inner.1, 4);
    }

    #[test]
    fn range_empty_window_stops_immediately() {
        let mut r = RangeFilter::new(
            Counting(csv_reader(FOUR), 0),
            ScanRange {
                start: 6,
                end: Some(6),
            },
        );
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 2);
    }

    #[test]
    fn range_beyond_eof_drains_and_returns_none() {
        let mut r = RangeFilter::new(
            Counting(csv_reader(FOUR), 0),
            ScanRange {
                start: 30,
                end: None,
            },
        );
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.inner.1, 5);
    }

    #[test]
    fn range_end_only_resolved_via_size() {
        let size: u64 = 22;
        let start = size - 9;
        assert_eq!(start, 13);
        let mut r = RangeFilter::new(csv_reader(FOUR), ScanRange { start, end: None });
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r1"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r2"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["r3"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_start_above_zero_yields_nothing() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let reader = json::Reader::new(
            Cursor::new(b"{\"a\": 1}\n".to_vec()),
            json::Params {
                ty: json::Type::Document,
            },
            &plan.from,
        );
        let mut r = RangeFilter::new(
            reader,
            ScanRange {
                start: 1,
                end: None,
            },
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn none_compression_passes_through() {
        let reader = decompressed(None, Box::new(Cursor::new(b"a\nb\n")));
        let mut r = csv::Reader::new(reader, params(None));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["a"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["b"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn gzip_round_trip() {
        let reader = decompressed(
            Some(Compression::Gzip),
            Box::new(Cursor::new(gzip(b"aaa\nbb\nc\n"))),
        );
        let mut r = csv::Reader::new(reader, params(None));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn gzip_multi_member_reads_all_streams() {
        let mut both = gzip(b"aaa\n");
        both.extend(gzip(b"bb\nc\n"));
        let reader = decompressed(Some(Compression::Gzip), Box::new(Cursor::new(both)));
        let mut r = csv::Reader::new(reader, params(None));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn bzip2_round_trip() {
        let reader = decompressed(
            Some(Compression::Bzip2),
            Box::new(Cursor::new(bzip2(b"aaa\nbb\nc\n"))),
        );
        let mut r = csv::Reader::new(reader, params(None));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn bzip2_multi_member_reads_all_streams() {
        let mut both = bzip2(b"aaa\n");
        both.extend(bzip2(b"bb\nc\n"));
        let reader = decompressed(Some(Compression::Bzip2), Box::new(Cursor::new(both)));
        let mut r = csv::Reader::new(reader, params(None));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["aaa"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["bb"], &["_1"]));
        assert_eq!(r.next().unwrap().unwrap(), csv(&["c"], &["_1"]));
        assert_eq!(r.next().unwrap(), None);
    }
}
