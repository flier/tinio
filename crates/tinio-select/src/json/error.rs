//! JSON input errors. Split out of the crate-level Error so every failure mode
//! keeps its own value and the original struson error (with its location)
//! survives instead of degrading into a formatted string.
//!
//! `From<ReaderError>` keeps a catch-all arm: `ReaderError` IS `#[non_exhaustive]`
//! in 0.7.2 (vendored `reader/mod.rs:513`), so the arm is mandatory and an
//! upgrade must not silently drop a variant.

use std::io;

use derive_more::PartialEq;
use struson::reader::{
    JsonReaderPosition, JsonSyntaxError, ReaderError, SyntaxErrorKind, UnexpectedStructureKind,
    ValueType,
};
use thiserror::Error;

use crate::{error::IoError, record::CAP_MESSAGE};

/// struson builds invalid UTF-8 as io::Error::new(ErrorKind::InvalidData,
/// "invalid UTF-8 data") (stream_reader.rs:1009). Both the kind and the message
/// are checked, so a reader failing with InvalidData for another reason stays a
/// plain IO error.
const INVALID_UTF8_MESSAGE: &str = "invalid UTF-8 data";

/// Compared by variant key AND payload, like the crate-level `Error`. Every
/// payload type is `PartialEq` (`ReaderError` itself is the only one that is
/// not), so two failures of the same kind are told apart by their location and
/// their kind/type/number fields. `Io` compares by `kind()` because its payload
/// is the crate's `IoError` newtype — an `std::io::Error` is not `PartialEq`,
/// which is what forced a hand-written impl before.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum Error {
    #[error("json input: {0}")]
    Syntax(#[from] JsonSyntaxError),
    #[error(
        "json input: unexpected value type: expected {expected:?}, got {actual:?} at {location}"
    )]
    UnexpectedType {
        expected: ValueType,
        actual: ValueType,
        location: JsonReaderPosition,
    },
    #[error("json input: unexpected structure: {kind} at {location}")]
    Structure {
        kind: UnexpectedStructureKind,
        location: JsonReaderPosition,
    },
    #[error("json input: nesting exceeds {max_nesting_depth} levels at {location}")]
    NestingDepth {
        max_nesting_depth: u32,
        location: JsonReaderPosition,
    },
    #[error("json input: unsupported number value: {number} at {location}")]
    Number {
        number: String,
        location: JsonReaderPosition,
    },
    /// Invalid UTF-8 in the input: an input-format error, not an IO failure.
    #[error("json input: invalid UTF-8 data at {location}")]
    InvalidUtf8 { location: JsonReaderPosition },
    /// A real failure of the underlying reader. The payload is the crate's
    /// `IoError` newtype rather than a bare `io::Error`, so the enum can derive
    /// `PartialEq`; its Display is transparent, so the rendered message is
    /// unchanged.
    #[error("S3 select: io error: {error}")]
    Io {
        error: IoError,
        location: JsonReaderPosition,
    },
    /// The 1 MB per-root record cap (design deviations 6 and 7).
    #[error("{CAP_MESSAGE}")]
    Cap,
    /// Forward-compatibility arm: `ReaderError` IS `#[non_exhaustive]`
    /// (vendored `reader/mod.rs:513`), so a future 0.7.x variant must still
    /// compile and must still report something.
    #[error("json input: {0}")]
    Other(String),
}

impl Error {
    /// The `IncompleteDocument` syntax failure — the one struson kind the
    /// kernel above this module acts on (`document::empty_or`'s empty-input
    /// rule), kept here so that no handler names a struson error type.
    pub(crate) fn is_incomplete_document(&self) -> bool {
        matches!(self, Self::Syntax(e) if e.kind == SyntaxErrorKind::IncompleteDocument)
    }
}

impl From<ReaderError> for Error {
    fn from(err: ReaderError) -> Self {
        match err {
            ReaderError::SyntaxError(e) => Self::Syntax(e),
            ReaderError::UnexpectedValueType {
                expected,
                actual,
                location,
            } => Self::UnexpectedType {
                expected,
                actual,
                location,
            },
            ReaderError::UnexpectedStructure { kind, location } => {
                Self::Structure { kind, location }
            }
            ReaderError::MaxNestingDepthExceeded {
                max_nesting_depth,
                location,
            } => Self::NestingDepth {
                max_nesting_depth,
                location,
            },
            ReaderError::UnsupportedNumberValue { number, location } => {
                Self::Number { number, location }
            }
            ReaderError::IoError { error, location } => {
                if error.kind() != io::ErrorKind::InvalidData {
                    Self::Io {
                        error: error.into(),
                        location,
                    }
                } else if error.to_string() == INVALID_UTF8_MESSAGE {
                    Self::InvalidUtf8 { location }
                } else if error.to_string() == CAP_MESSAGE {
                    // The charged `Read` backstop in `json::feed` and the line
                    // framing raise the pinned `CAP_MESSAGE` with `InvalidData`
                    // while a root is being served, so struson surfaces them as an
                    // `IoError`. Recognizing that exact kind + message maps them
                    // back to the cap variant, which keeps the crate-level Display
                    // byte-identical to the retired kernel's rendering.
                    Self::Cap
                } else {
                    Self::Io {
                        error: error.into(),
                        location,
                    }
                }
            }
            // `ReaderError` is `#[non_exhaustive]`: without this arm the match
            // does not compile downstream (E0004), and a future variant must
            // not be dropped silently.
            other => Self::Other(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use struson::reader::{
        JsonReaderPosition, JsonSyntaxError, ReaderError, SyntaxErrorKind, UnexpectedStructureKind,
        ValueType,
    };

    use super::Error;
    use crate::{
        json::testing::{io_error, location},
        record::CAP_MESSAGE,
    };

    /// A position distinguishable from `location()`'s. `data_pos` is the only
    /// field these assertions tell apart, and what the rendered message ends
    /// with.
    fn at(data_pos: u64) -> JsonReaderPosition {
        let mut location = location();
        location.data_pos = Some(data_pos);
        location
    }

    fn syntax(kind: SyntaxErrorKind) -> ReaderError {
        ReaderError::SyntaxError(JsonSyntaxError {
            kind,
            location: location(),
        })
    }

    #[test]
    fn cap_renders_the_verbatim_crate_message() {
        assert_eq!(CAP_MESSAGE, "input record exceeds 1 MB");
        assert_eq!(Error::Cap.to_string(), CAP_MESSAGE);
    }

    #[test]
    fn syntax_renders_the_json_input_prefix() {
        let json = Error::from(syntax(SyntaxErrorKind::TrailingData));
        assert!(json.to_string().starts_with("json input:"), "{json}");
    }

    #[test]
    fn unexpected_value_type_carries_the_types_and_location() {
        // A typed mismatch (a `next_string()` over a number, say) is the one
        // variant whose payload is two types: both are named in the rendered
        // message, so a struson upgrade that renamed or dropped a field must
        // fail here instead of rendering a hole. The location rides along too.
        let location = at(7);
        let json = Error::from(ReaderError::UnexpectedValueType {
            expected: ValueType::String,
            actual: ValueType::Number,
            location: location.clone(),
        });
        assert_eq!(
            json,
            Error::UnexpectedType {
                expected: ValueType::String,
                actual: ValueType::Number,
                location: location.clone(),
            }
        );
        assert_eq!(
            json.to_string(),
            format!("json input: unexpected value type: expected String, got Number at {location}")
        );
    }

    #[test]
    fn unexpected_structure_carries_the_kind_and_location() {
        // `UnexpectedStructureKind` is what says *which* element the reader
        // wanted (here the index of the missing one), so mapping it to a bare
        // `Structure` would drop the half of the error a caller acts on.
        let location = at(11);
        let kind = UnexpectedStructureKind::TooShortArray { expected_index: 2 };
        let json = Error::from(ReaderError::UnexpectedStructure {
            kind: kind.clone(),
            location: location.clone(),
        });
        assert_eq!(
            json,
            Error::Structure {
                kind,
                location: location.clone(),
            }
        );
        assert_eq!(
            json.to_string(),
            format!(
                "json input: unexpected structure: TooShortArray(expected_index = 2) at {location}"
            )
        );
    }

    #[test]
    fn unsupported_number_value_carries_the_token_and_location() {
        // The token itself is the payload: rejecting a 30-digit integer is
        // only actionable if the message quotes it. struson raises this only
        // while `restrict_number_values` is on, and `json::source::settings`
        // pins it off (fact 11: `1e309` and long integers must parse), so
        // this arm has no runtime route and is constructed directly — which
        // is exactly why a settings change would otherwise slip past silent.
        let number = "123456789012345678901234567890";
        let location = at(3);
        let json = Error::from(ReaderError::UnsupportedNumberValue {
            number: number.into(),
            location: location.clone(),
        });
        assert_eq!(
            json,
            Error::Number {
                number: number.into(),
                location: location.clone(),
            }
        );
        assert_eq!(
            json.to_string(),
            format!("json input: unsupported number value: {number} at {location}")
        );
    }

    #[test]
    fn io_errors_compare_by_kind_not_message() {
        // `Io`'s payload is the crate's `IoError` newtype, whose PartialEq
        // compares by `kind()`: the semantics the deleted hand-written impl
        // gave this variant, now reached through the derive.
        let a = Error::from(io_error(io::ErrorKind::ConnectionReset, "boom"));
        let b = Error::from(io_error(io::ErrorKind::ConnectionReset, "different"));
        assert_eq!(a, b);
        assert_ne!(a, Error::from(io_error(io::ErrorKind::BrokenPipe, "boom")));
    }

    #[test]
    fn invalid_utf8_is_recognized_by_io_kind_and_message() {
        let json = Error::from(io_error(io::ErrorKind::InvalidData, "invalid UTF-8 data"));
        assert!(matches!(json, Error::InvalidUtf8 { .. }), "{json:?}");
    }

    #[test]
    fn a_real_read_failure_stays_an_io_error() {
        let json = Error::from(io_error(io::ErrorKind::ConnectionReset, "boom"));
        match json {
            Error::Io { error, .. } => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[test]
    fn invalid_data_from_the_reader_is_not_called_invalid_utf8() {
        // A reader that genuinely fails with InvalidData and a different
        // message must not be reported as a malformed document.
        let json = Error::from(io_error(io::ErrorKind::InvalidData, "disk on fire"));
        assert!(matches!(json, Error::Io { .. }), "{json:?}");
    }

    #[test]
    fn the_backstop_cap_message_maps_to_the_cap_variant() {
        // The charged Read backstop raises InvalidData + CAP_MESSAGE mid-read,
        // so struson wraps it in an IoError; it must surface as the cap, not as
        // an IO failure.
        let json = Error::from(io_error(io::ErrorKind::InvalidData, CAP_MESSAGE));
        assert!(matches!(json, Error::Cap), "{json:?}");
    }

    #[test]
    fn partial_eq_compares_the_variant_and_payload() {
        assert_eq!(Error::Cap, Error::Cap);
        assert_ne!(
            Error::Cap,
            Error::from(syntax(SyntaxErrorKind::TrailingData))
        );
        // The point of the payload comparison: the same variant with a
        // different SyntaxErrorKind is no longer equal.
        assert_ne!(
            Error::from(syntax(SyntaxErrorKind::TrailingData)),
            Error::from(syntax(SyntaxErrorKind::IncompleteDocument))
        );
        assert_eq!(
            Error::from(syntax(SyntaxErrorKind::TrailingData)),
            Error::from(syntax(SyntaxErrorKind::TrailingData))
        );
        // ... and the location participates too: `location()` is built over an
        // empty document, so a second, distinct position is unequal to it.
        let mut other_location = location();
        other_location.data_pos = Some(7);
        assert_ne!(
            Error::from(syntax(SyntaxErrorKind::TrailingData)),
            Error::from(ReaderError::SyntaxError(JsonSyntaxError {
                kind: SyntaxErrorKind::TrailingData,
                location: other_location,
            }))
        );
    }
}
