//! FROM object-path segments (`PathSeg`), pest grammar (jsonpath-rust
//! layout: grammar file + #[derive(Parser)] + manual Pair→model), plus the
//! monotonic (line,col)→byte cursor the dialect uses to slice the pest
//! prefix. The dialect uses the prefix-parse `path` rule for consumed
//! length; the `main` rule (EOI) is the test entry.

use pest::{Parser, iterators::Pair};
use pest_derive::Parser;

/// One step of the FROM object path (`S3Object[*].books[0]`); `Index` is
/// 0-based.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSeg {
    Name(String),
    Index(usize),
    Wild,
}

/// Shared rejection text for the whole FROM-path family: the dialect's
/// custom-factor parser returns the same message for every refused path
/// (published here once, referenced from both modules — review R2).
pub(crate) const INVALID_FROM_PATH: &str = "invalid FROM path";

impl PathSeg {
    /// Prefix match (`path` rule, no EOI) — used by the dialect.
    /// Returns (segments, consumed byte length).
    pub(crate) fn parse(input: &str) -> Result<(Vec<Self>, usize), String> {
        let pairs = ObjectPathParser::parse(Rule::path, input)
            .map_err(|e| format!("{INVALID_FROM_PATH}: {e}"))?;
        let pair = pairs
            .into_iter()
            .next()
            .ok_or_else(|| INVALID_FROM_PATH.to_string())?;
        let len = pair.as_span().end();
        let segs = walk_path_pair(pair)?;
        Ok((segs, len))
    }
}

#[derive(Parser)]
#[grammar = "grammar/object_path.pest"]
struct ObjectPathParser;

fn walk_path_pair(pair: Pair<'_, Rule>) -> Result<Vec<PathSeg>, String> {
    let mut out = Vec::new();
    for seg in pair.into_inner() {
        match seg.as_rule() {
            // Container rules: recurse AND merge — `first_seg` yields `wstar`,
            // `bracketed` yields `wstar`/`index`/`quoted`. Dropping the
            // recursion result silently drops segments.
            Rule::path | Rule::first_seg | Rule::bracketed => {
                out.extend(walk_path_pair(seg)?);
            }
            Rule::dot_name => {
                let p = seg.into_inner().next().unwrap(); // name
                out.push(PathSeg::Name(p.as_str().to_string()));
            }
            Rule::dot_wild | Rule::wstar => out.push(PathSeg::Wild),
            Rule::index => {
                let v: usize = seg
                    .as_str()
                    .parse()
                    .map_err(|_| INVALID_FROM_PATH.to_string())?;
                out.push(PathSeg::Index(v));
            }
            Rule::quoted => {
                // `['name']`: raw content (no escapes), outer quotes stripped.
                let s = seg.as_str();
                let name = s.trim_matches('\'').to_string();
                out.push(PathSeg::Name(name));
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Monotonic (line, col) → byte cursor. 0.62 spans carry only line/column,
/// so slicing needs this converter; the parser consumes token spans in
/// strictly increasing document order, so one cursor walking forward serves
/// a whole factor parse — each seek walks only the gap since the previous
/// one, O(statement) total, no allocation and no per-parse map (an earlier
/// revision built a HashMap entry per char; the one after it re-walked from
/// byte 0 per lookup, quadratic on a long path — 2026-09-10).
pub(crate) struct SpanCursor<'a> {
    sql: &'a str,
    line: u64,
    col: u64,
    offset: usize,
}

impl<'a> SpanCursor<'a> {
    pub(crate) fn new(sql: &'a str) -> Self {
        Self {
            sql,
            line: 1,
            col: 1,
            offset: 0,
        }
    }

    /// Byte offset of (line, col); (1, 1) is 0, columns count chars (matching
    /// tokenizer spans). `None` when the target precedes the cursor (callers
    /// only ever move forward) or lies past the end of input.
    pub(crate) fn seek(&mut self, line: u64, col: u64) -> Option<usize> {
        if (line, col) < (self.line, self.col) {
            return None;
        }
        while (self.line, self.col) != (line, col) {
            let ch = self.sql[self.offset..].chars().next()?;
            self.offset += ch.len_utf8();
            if ch == '\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
        Some(self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full-text match (`main` rule, with EOI) — test-only entry (review
    /// R3: the grammar's `main` rules exist for the tests; the dialect
    /// uses the prefix `PathSeg::parse` only).
    fn validate_full(input: &str) -> Result<Vec<PathSeg>, String> {
        let mut pairs = ObjectPathParser::parse(Rule::main, input)
            .map_err(|e| format!("{INVALID_FROM_PATH}: {e}"))?;
        let path = pairs
            .next()
            .ok_or_else(|| INVALID_FROM_PATH.to_string())?
            .into_inner()
            .next()
            .ok_or_else(|| INVALID_FROM_PATH.to_string())?;
        walk_path_pair(path)
    }

    fn ok(s: &str) -> Vec<PathSeg> {
        validate_full(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn rej(s: &str) {
        assert!(validate_full(s).is_err(), "{s}: expected rejection");
    }

    #[test]
    fn s3object_alone_is_valid() {
        assert_eq!(ok("S3Object"), vec![PathSeg::Wild; 0]);
    }

    #[test]
    fn wildcard_first_segment() {
        // Pins the walker merge: `first_seg` recurses to `wstar`.
        assert_eq!(ok("S3Object[*]"), vec![PathSeg::Wild]);
    }

    #[test]
    fn traversal_path_full() {
        assert_eq!(
            ok("S3Object[*].books[*].price"),
            vec![
                PathSeg::Wild,
                PathSeg::Name("books".into()),
                PathSeg::Wild,
                PathSeg::Name("price".into()),
            ]
        );
    }

    #[test]
    fn index_segment() {
        // Pins the walker merge: `bracketed` recurses to `index`.
        assert_eq!(
            ok("S3Object[*].books[0]"),
            vec![
                PathSeg::Wild,
                PathSeg::Name("books".into()),
                PathSeg::Index(0)
            ]
        );
    }

    #[test]
    fn quoted_name_segment() {
        assert_eq!(
            ok("S3Object[*]['a b']"),
            vec![PathSeg::Wild, PathSeg::Name("a b".into())]
        );
    }

    #[test]
    fn dot_wild_segment() {
        assert_eq!(ok("S3Object[*].*"), vec![PathSeg::Wild, PathSeg::Wild]);
    }

    #[test]
    fn case_insensitive_root() {
        assert_eq!(
            ok("s3object[*].name"),
            vec![PathSeg::Wild, PathSeg::Name("name".into())]
        );
    }

    #[test]
    fn dollar_in_names() {
        assert_eq!(
            ok("S3Object[*].a$b"),
            vec![PathSeg::Wild, PathSeg::Name("a$b".into())]
        );
    }

    #[test]
    fn no_dot_first_segment() {
        rej("S3Object.name");
    }

    #[test]
    fn no_index_first_segment() {
        rej("S3Object[0]");
    }

    #[test]
    fn index_overflow_is_rejected() {
        rej("S3Object[*].books[99999999999999999999]");
    }

    #[test]
    fn unterminated_quoted_segment_is_rejected() {
        // pest-unit-level only: end-to-end this input dies in parse_sql's
        // upfront tokenization before any hook runs.
        rej("S3Object[*].'books");
    }

    #[test]
    fn quoted_content_with_quote_is_rejected() {
        // No-escape rule: raw up to the closing quote.
        rej("S3Object[*]['it''s']");
    }

    #[test]
    fn affix_parse_consumes_only_path() {
        let (segs, len) = PathSeg::parse("S3Object[*].books[*].price s WHERE x = 1").unwrap();
        assert_eq!(segs.len(), 4);
        assert_eq!(len, "S3Object[*].books[*].price".len());
    }

    #[test]
    fn prefix_parse_plain_factor() {
        let (segs, len) = PathSeg::parse("S3Object s").unwrap();
        assert!(segs.is_empty());
        assert_eq!(len, "S3Object".len());
    }

    #[test]
    fn non_ascii_is_rejected() {
        rej("S3Object[*].café");
    }

    #[test]
    fn whitespace_not_allowed() {
        rej("S3Object [*]");
    }

    #[test]
    fn span_cursor_walks_forward_only() {
        // "abc\ndef": (1,1)=0, (1,4)=3 (the '\n' byte), (2,1)=4, (2,4)=7
        // (one past the last char — the end-of-input position the
        // zero-segment `last_end` seed needs).
        let mut cursor = SpanCursor::new("abc\ndef");
        assert_eq!(cursor.seek(1, 1), Some(0));
        assert_eq!(cursor.seek(1, 4), Some(3));
        assert_eq!(cursor.seek(2, 1), Some(4));
        assert_eq!(cursor.seek(2, 4), Some(7));
        // Backwards seeks are refused and leave the cursor where it was.
        assert_eq!(cursor.seek(1, 1), None);
        assert_eq!(cursor.seek(2, 3), None);
        assert_eq!(cursor.seek(2, 4), Some(7));
        // Past the end of input: refused.
        assert_eq!(cursor.seek(2, 5), None);
        assert_eq!(cursor.seek(3, 1), None);

        // Columns count chars, not bytes ('ä' is 2 bytes): (1,2)=2.
        let mut cursor = SpanCursor::new("äh");
        assert_eq!(cursor.seek(1, 2), Some(2));
        assert_eq!(cursor.seek(1, 3), Some(3));
    }
}
