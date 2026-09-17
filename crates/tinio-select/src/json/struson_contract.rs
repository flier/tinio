//! Contract tests pinning the struson 0.7.2 behaviours this crate depends on.
//! They drive JsonStreamReader directly, not our wrappers, so an upstream
//! upgrade fails here first with a clear name.
//!
//! Source of truth: struson 0.7.2 as vendored in the local cargo registry.
//! docs.rs latest and GitHub main are 0.8-dev and differ.
//! See the design spec, verified facts 1/2/3/4/5/6/9/10.
//!
//! Fact 12 (prose pin, deliberately not asserted below): the pinned release is
//! struson **0.7.2**, dual-licensed **MIT OR Apache-2.0**, **MSRV 1.85**. Those
//! three are properties of the upstream release, not observable behaviour, so
//! they are recorded here rather than exercised by a test.

use std::io::{ErrorKind, Read};

use struson::reader::{JsonReader, JsonStreamReader, ReaderError, ReaderSettings, SyntaxErrorKind};

/// Deliberately local: this file pins upstream behaviour, so it must not read
/// `data_pos` through the crate's own `json::source` helper — a bug there would
/// otherwise mask the upstream change the guard exists to catch.
fn pos<R: Read>(reader: &JsonStreamReader<R>) -> u64 {
    reader
        .current_position(false)
        .data_pos
        .expect("JsonStreamReader always reports data_pos")
}

/// Fact 1: the 0.7.2 field set and its defaults.
#[test]
fn settings_defaults_match_the_pinned_facts() {
    let settings = ReaderSettings::default();
    assert!(!settings.allow_comments);
    assert!(!settings.allow_trailing_comma);
    assert!(!settings.allow_multiple_top_level);
    assert!(settings.track_path, "0.7.2 default is true");
    assert_eq!(settings.max_nesting_depth, Some(128));
    assert!(settings.restrict_number_values, "0.7.2 default is true");
}

/// Fact 3: peek() never advances data_pos, whatever the value kind.
#[test]
fn peek_never_moves_data_pos() {
    for json in [
        "{\"a\":1}",
        "[1,2]",
        "\"hi\"",
        "12.0e5",
        "true",
        "false",
        "null",
    ] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        let before = pos(&reader);
        reader.peek().expect("peek");
        assert_eq!(pos(&reader), before, "{json}: peek moved data_pos");
    }
}

/// Fact 4: consume_trailing_whitespace is the only safe trailing probe.
#[test]
fn trailing_data_is_reported_by_consume_trailing_whitespace() {
    for json in ["{} x", "5 {}", "[1] x"] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        reader.skip_value().expect("root value");
        match reader.consume_trailing_whitespace() {
            Err(ReaderError::SyntaxError(e)) => {
                assert_eq!(e.kind, SyntaxErrorKind::TrailingData, "{json}");
            }
            other => panic!("{json}: expected TrailingData, got {other:?}"),
        }
    }
    let mut reader = JsonStreamReader::new(b"{} ".as_slice());
    reader.skip_value().expect("root value");
    reader
        .consume_trailing_whitespace()
        .expect("trailing whitespace is clean");
}

/// Fact 9: a literal followed by a stray byte fails during the value read.
#[test]
fn trailing_data_after_literal_fails_at_the_value() {
    let mut reader = JsonStreamReader::new(b"truey".as_slice());
    match reader.next_bool() {
        Err(ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::TrailingDataAfterLiteral);
        }
        other => panic!("expected TrailingDataAfterLiteral, got {other:?}"),
    }
    let mut reader = JsonStreamReader::new(b"nullx".as_slice());
    match reader.next_null() {
        Err(ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::TrailingDataAfterLiteral);
        }
        other => panic!("expected TrailingDataAfterLiteral, got {other:?}"),
    }
}

/// Fact 10: empty and whitespace-only inputs both fail at the first peek.
#[test]
fn empty_and_whitespace_only_fail_at_peek() {
    for json in ["", "   \n  "] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        match reader.peek() {
            Err(ReaderError::SyntaxError(e)) => {
                assert_eq!(e.kind, SyntaxErrorKind::IncompleteDocument, "{json:?}");
            }
            other => panic!("{json:?}: expected IncompleteDocument, got {other:?}"),
        }
    }
}

/// Fact 6: invalid UTF-8 arrives as an IoError whose kind is InvalidData.
#[test]
fn invalid_utf8_is_an_invalid_data_io_error() {
    let bytes = b"{\"a\":\"\xff\xfe\"}".to_vec();
    let mut reader = JsonStreamReader::new(&bytes[..]);
    reader.begin_object().expect("begin object");
    reader.next_name_owned().expect("name");
    match reader.next_string() {
        Err(ReaderError::IoError { error, .. }) => {
            assert_eq!(error.kind(), ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "invalid UTF-8 data");
        }
        other => panic!("expected IoError/InvalidData, got {other:?}"),
    }
}

/// Fact 5: a lone surrogate is rejected on the skipped path too.
#[test]
fn skipped_subtree_rejects_a_lone_surrogate() {
    let json = "{\"b\":\"\\uD800\",\"a\":1}";
    let mut reader = JsonStreamReader::new(json.as_bytes());
    reader.begin_object().expect("begin object");
    assert_eq!(reader.next_name_owned().expect("name"), "b");
    match reader.skip_value() {
        Err(ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::UnpairedSurrogatePairEscapeSequence);
        }
        other => panic!("expected an unpaired-surrogate error, got {other:?}"),
    }
}

/// Fact 2 (spot check): current_position(false) reports no path.
#[test]
fn current_position_without_path_is_available() {
    let reader = JsonStreamReader::new(b"{}".as_slice());
    let position = reader.current_position(false);
    assert!(position.path.is_none());
    assert!(position.line_pos.is_some());
    assert_eq!(position.data_pos, Some(0));
}
