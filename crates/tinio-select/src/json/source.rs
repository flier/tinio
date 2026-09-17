//! Shared seam over the struson kernel: the reader settings both handlers
//! build, the two classifications of a struson `ReaderError` they need — the
//! error mapping and the terminal trailing probe — and the one `ValueSource`
//! adapter they drive.
//!
//! Both classifications live here, and the `ReaderError` mapping itself in
//! `json::error`, so no module above the seam names a struson error type;
//! `document`, `lines` and `matcher` see `crate::Error` only. `Reader` below
//! carries the same rule for the reader type.
//!
//! `ReaderSource` holds a **reborrow** of a `JsonStreamReader`, so a single
//! adapter serves both framings: LINES owns its reader for one line and lends
//! it per pull, while DOCUMENT keeps the one reader of the whole document and
//! borrows it for each pull.

use std::{borrow::Cow, io::Read};

use struson::reader::{
    JsonReader, JsonStreamReader, ReaderError, ReaderSettings, SyntaxErrorKind, ValueType,
};

use crate::{
    error::Error,
    json::{
        error::Error as JsonError,
        matcher::{ValueKind, ValueSource},
    },
};

/// Map a struson error into the crate error.
///
/// A **real read failure** must keep the crate IO shape instead of nesting into
/// `Error::Json(json::Error::Io)`, which would render as
/// `S3 select: input error: S3 select: io error: ...` (double prefix). Invalid
/// UTF-8 is already split off inside `json::Error::from`, so what reaches the
/// `Io` arm here is a genuine reader failure.
pub(crate) fn map_json_err(err: ReaderError) -> Error {
    match JsonError::from(err) {
        JsonError::Io { error, .. } => Error::from(error),
        other => Error::Json(other.into()),
    }
}

/// The terminal trailing probe: consume the whitespace after the top-level
/// value and classify its one failure, which is *ours*, not struson's.
///
/// struson reports every trailing non-whitespace byte as `TrailingData`
/// (probe fact 9 — a parseable second value and plain garbage are the same
/// error through its public API), so that shape becomes the caller's own
/// `message`: bare text, which `JsonError::Other` renders as `json input: {0}`
/// (deviation ⑨; the wording is pinned verbatim per handler). Anything else is
/// a real failure and goes through `map_json_err`.
///
/// By value, because `consume_trailing_whitespace` consumes the reader — the
/// caller has already `take()`n it out of its `Option`. `tail.done` stays with
/// the caller, since only DOCUMENT latches it on a clean tail (LINES reads the
/// line end off the framing's EOF), so the result is returned unmodified.
pub(crate) fn consume_trailing<R: Read>(
    reader: JsonStreamReader<R>,
    message: &str,
) -> Result<(), Error> {
    match reader.consume_trailing_whitespace() {
        Ok(()) => Ok(()),
        Err(ReaderError::SyntaxError(e)) if e.kind == SyntaxErrorKind::TrailingData => {
            Err(Error::Json(JsonError::Other(message.into()).into()))
        }
        Err(e) => Err(map_json_err(e)),
    }
}

/// Reader settings the kernel uses, with all six fields written out so an
/// upstream default change cannot silently alter behaviour: no comments, no
/// trailing comma, a single top-level value, no JSON-path copy (we never read
/// it), the depth limit of 128 (deviation ⑪), and unrestricted number tokens
/// (fact 11 — otherwise `1e309` and 30-digit integers are rejected).
pub(crate) fn settings() -> ReaderSettings {
    ReaderSettings {
        allow_comments: false,
        allow_trailing_comma: false,
        allow_multiple_top_level: false,
        track_path: false,
        max_nesting_depth: Some(128),
        restrict_number_values: false,
    }
}

/// `current_position(false).data_pos`: the offset of the last consumed byte,
/// which struson always reports for a stream reader. The handlers' own free
/// function: the matcher never samples a position, so it is not a
/// `ValueSource` member.
pub(crate) fn position<R: JsonReader>(reader: &R) -> u64 {
    reader
        .current_position(false)
        .data_pos
        .expect("JsonStreamReader always reports data_pos")
}

/// The stream reader both handlers drive, named here so that the same rule that
/// keeps struson *errors* below the seam keeps its reader type below it too.
pub(crate) type Reader<R> = JsonStreamReader<R>;

/// The `ValueSource` over a struson reader: each call routes its one struson
/// error through `map_json_err`, so nothing above the seam sees a
/// `ReaderError` (see the module note for the other classifier).
pub(crate) struct ReaderSource<'a, R: JsonReader> {
    reader: &'a mut R,
}

impl<'a, R: JsonReader> ReaderSource<'a, R> {
    pub(crate) fn new(reader: &'a mut R) -> Self {
        Self { reader }
    }
}

impl<R: JsonReader> ValueSource for ReaderSource<'_, R> {
    fn peek(&mut self) -> Result<ValueKind, Error> {
        Ok(match self.reader.peek().map_err(map_json_err)? {
            ValueType::Array => ValueKind::Array,
            ValueType::Object => ValueKind::Object,
            ValueType::String => ValueKind::String,
            ValueType::Number => ValueKind::Number,
            ValueType::Boolean => ValueKind::Bool,
            ValueType::Null => ValueKind::Null,
        })
    }

    fn has_next(&mut self) -> Result<bool, Error> {
        self.reader.has_next().map_err(map_json_err)
    }

    fn name(&mut self) -> Result<Cow<'_, str>, Error> {
        // The allocation-free form: `next_name_owned` copies every member name
        // the path walks, while the matcher only compares the name and drops it
        // (with `track_path` off struson skips its own copy too).
        self.reader
            .next_name()
            .map_err(map_json_err)
            .map(Cow::Borrowed)
    }

    fn text(&mut self) -> Result<String, Error> {
        self.reader.next_string().map_err(map_json_err)
    }

    fn number(&mut self) -> Result<String, Error> {
        // Verbatim token: next_number_as_str never round-trips through f64/u64.
        Ok(self
            .reader
            .next_number_as_str()
            .map_err(map_json_err)?
            .to_owned())
    }

    fn boolean(&mut self) -> Result<bool, Error> {
        self.reader.next_bool().map_err(map_json_err)
    }

    fn null(&mut self) -> Result<(), Error> {
        self.reader.next_null().map_err(map_json_err)
    }

    fn begin_object(&mut self) -> Result<(), Error> {
        self.reader.begin_object().map_err(map_json_err)
    }

    fn end_object(&mut self) -> Result<(), Error> {
        self.reader.end_object().map_err(map_json_err)
    }

    fn begin_array(&mut self) -> Result<(), Error> {
        self.reader.begin_array().map_err(map_json_err)
    }

    fn end_array(&mut self) -> Result<(), Error> {
        self.reader.end_array().map_err(map_json_err)
    }

    fn skip_value(&mut self) -> Result<(), Error> {
        self.reader.skip_value().map_err(map_json_err)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::map_json_err;
    use crate::{
        error::Error,
        json::{error::Error as JsonError, testing::io_error},
    };

    #[test]
    fn a_reader_failure_keeps_the_crate_io_shape() {
        // `Error::from` accepts any `io::Error`, so `Error::from(error)` here
        // yields the crate-level `Io` and the message keeps one prefix. The
        // nested form — `Error::Json(json::Error::Io)` — would render as
        // `S3 select: input error: S3 select: io error: ...`, one prefix per
        // layer, and is what this arm exists to prevent.
        let err = map_json_err(io_error(io::ErrorKind::ConnectionReset, "connection reset"));
        match &err {
            Error::Io(io) => assert_eq!(io.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected the crate-level Io, got {other:?}"),
        }
        let message = err.to_string();
        assert_eq!(message, "S3 select: io error: connection reset");
        assert_eq!(message.matches("S3 select:").count(), 1, "{message}");
    }

    #[test]
    fn reader_invalid_utf8_is_a_json_error_not_an_io_error() {
        // struson raises malformed UTF-8 as `IoError` with `InvalidData` plus
        // the exact message "invalid UTF-8 data" (`stream_reader.rs:1009`),
        // and `json::Error::from` splits that pair off before `map_json_err`
        // sees it. It is an input-format failure: the contrasting case that
        // proves the arm above is not simply "every IoError becomes Io".
        let err = map_json_err(io_error(io::ErrorKind::InvalidData, "invalid UTF-8 data"));
        match &err {
            Error::Json(input) => assert!(
                matches!(input.0, JsonError::InvalidUtf8 { .. }),
                "{input:?}"
            ),
            other => panic!("expected Error::Json(InvalidUtf8), got {other:?}"),
        }
        assert!(
            err.to_string()
                .starts_with("S3 select: input error: json input: invalid UTF-8 data"),
            "{err}"
        );
    }
}
