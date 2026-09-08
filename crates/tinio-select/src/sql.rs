//! SQL parse + validate: a FROM-path preprocessor, a quote-aware
//! `IS [NOT] MISSING` rewrite, then sqlparser + a restrict-grammar
//! validator producing the engine's `QueryPlan`.

use std::{collections::HashMap, ops::ControlFlow};

use sqlparser::{
    ast::{
        DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
        LimitClause, SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor,
    },
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

use crate::error::Error;

/// One step of the FROM object path (`S3Object[*].books[0]`); `Index` is
/// 0-based.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSeg {
    Name(String),
    Index(usize),
    Wild,
}

/// The parsed FROM clause: the S3Object path walk plus the optional alias.
/// The `segments` list is the whole traversal state (a non-traversed clause
/// has an empty list) — a separate `traversed` flag was derived state,
/// removed in the review (2026-09-06 simplify).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub segments: Vec<PathSeg>,
    pub alias: Option<String>,
}

/// One SELECT-list item.
// The plan's pinned interface keeps `Item` owning the parsed `Expr`
// (Task 5 evaluates it directly); the size gap vs the unit `Wild` variant
// is the price of that shape.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Wild,
    Item { expr: Expr, alias: Option<String> },
}

/// Validated query plan consumed by the engine (`eval`, `json::Reader`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub from: FromClause,
    pub projections: Vec<Projection>,
    pub where_expr: Option<Expr>,
    pub limit: Option<usize>,
    pub aggregates: bool,
}

/// AWS documented expression limit (review 2026-09-05 #5).
const MAX_EXPRESSION: usize = 256 * 1024;

const S3_OBJECT: &str = "S3Object";

/// Keywords that can never serve as the FROM alias (they start the next
/// clause, or they are join/table operators).
const CLAUSE_KEYWORDS: &[&str] = &[
    "where",
    "group",
    "order",
    "having",
    "limit",
    "union",
    "except",
    "intersect",
    "join",
    "left",
    "right",
    "inner",
    "cross",
    "full",
    "natural",
    "on",
    "using",
    "offset",
    "fetch",
    "window",
    "qualify",
    "settings",
    "format",
];

/// Keywords that terminate an operand walk-back in the `IS [NOT] MISSING`
/// rewrite: everything that binds looser than `IS`.
const OPERAND_BOUNDARY_KEYWORDS: &[&str] = &[
    "and", "or", "not", "where", "select", "from", "limit", "when", "then", "else",
];

/// Parse + validate one S3 Select expression.
pub fn parse(sql: &str) -> Result<QueryPlan, Error> {
    if sql.len() > MAX_EXPRESSION {
        return Err(Error::Parse("expression exceeds 256 KiB".into()));
    }
    // Before the MISSING rewrite (which inserts the sentinel names): a
    // user-authored sentinel call would silently adopt MISSING semantics.
    reject_reserved_functions(sql)?;
    let (from, rewritten) = preprocess_from(sql)?;
    let rewritten = rewrite_is_missing(&rewritten)?;
    let stmts = Parser::parse_sql(&GenericDialect, &rewritten)
        .map_err(|e| Error::Parse(format!("syntax error: {e}")))?;
    let stmt = match stmts.as_slice() {
        [s] => s,
        [] => return Err(Error::Parse("empty expression".into())),
        _ => return Err(Error::Parse("expression must be a single statement".into())),
    };
    let Statement::Query(query) = stmt else {
        return Err(Error::Parse(format!("invalid statement: {stmt}")));
    };
    if query.with.is_some() {
        return Err(Error::Unsupported("WITH".into()));
    }
    if query.order_by.is_some() {
        return Err(Error::Unsupported("ORDER BY".into()));
    }
    let select = match &*query.body {
        SetExpr::Select(s) => s.as_ref(),
        SetExpr::SetOperation { .. } => return Err(Error::Unsupported("UNION".into())),
        SetExpr::Query(_) => return Err(Error::Unsupported("subquery".into())),
        _ => {
            return Err(Error::Parse(format!(
                "unexpected statement: {}",
                query.body
            )));
        }
    };
    if select.distinct.is_some() {
        return Err(Error::Unsupported("DISTINCT".into()));
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers)
            if !exprs.is_empty() || !modifiers.is_empty() =>
        {
            return Err(Error::Unsupported("GROUP BY".into()));
        }
        GroupByExpr::All(_) => return Err(Error::Unsupported("GROUP BY".into())),
        GroupByExpr::Expressions(_, _) => {}
    }
    if select.having.is_some() {
        return Err(Error::Unsupported("HAVING".into()));
    }
    let limit = match &query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            limit_by,
        }) => {
            if offset.is_some() {
                return Err(Error::Unsupported("OFFSET".into()));
            }
            if !limit_by.is_empty() {
                return Err(Error::Unsupported("LIMIT BY".into()));
            }
            positive_limit(limit)?
        }
        Some(LimitClause::LimitOffset { limit: None, .. }) => {
            return Err(Error::Parse("LIMIT must be a positive integer".into()));
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            return Err(Error::Parse("LIMIT must be a positive integer".into()));
        }
    };
    validate_from(select, &from)?;
    // Aggregates are projection-derived (`plan.aggregates` below); a WHERE
    // aggregate would surface in-stream as an internal engine error instead
    // of a request-level parse error — refused here (review 2026-09-05b).
    if let Some(where_expr) = &select.selection {
        if contains_aggregate(where_expr) {
            return Err(Error::Parse("aggregates not allowed in WHERE".into()));
        }
        reject_subqueries(where_expr)?;
    }
    let mut projections = Vec::with_capacity(select.projection.len());
    let mut aggregates = false;
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::Wildcard(_) => {
                projections.push(Projection::Wild);
                continue;
            }
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(Error::Unsupported("qualified wildcard".into()));
            }
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            // Spark-style `expr AS (a, b)` (sqlparser 0.62) — outside the
            // AWS surface, refused like the other unparsed shapes.
            SelectItem::ExprWithAliases { .. } => {
                return Err(Error::Parse(
                    "alias list projection is not supported".into(),
                ));
            }
        };
        aggregates |= contains_aggregate(expr);
        reject_subqueries(expr)?;
        projections.push(Projection::Item {
            expr: expr.clone(),
            alias,
        });
    }
    if aggregates && projections.iter().any(|p| matches!(p, Projection::Wild)) {
        return Err(Error::Parse(
            "aggregates require an explicit select list".into(),
        ));
    }
    if aggregates {
        for item in &projections {
            let Projection::Item { expr, .. } = item else {
                unreachable!("aggregates+Wild rejected above");
            };
            validate_aggregate_item(expr)?;
        }
    }
    Ok(QueryPlan {
        from,
        projections,
        where_expr: select.selection.clone(),
        limit,
        aggregates,
    })
}

/// LIMIT must be a positive integer literal.
fn positive_limit(limit: &Expr) -> Result<Option<usize>, Error> {
    match limit {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => match n.parse::<usize>() {
                Ok(n) if n > 0 => Ok(Some(n)),
                _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
            },
            _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
        },
        _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
    }
}

/// The FROM clause must be exactly the single `S3Object` factor (with the
/// alias the preprocessor recorded).
fn validate_from(select: &sqlparser::ast::Select, clause: &FromClause) -> Result<(), Error> {
    let twj = match select.from.as_slice() {
        [t] => t,
        _ => {
            return Err(Error::Parse(
                "FROM must reference exactly one S3Object".into(),
            ));
        }
    };
    if !twj.joins.is_empty() {
        return Err(Error::Unsupported("JOIN".into()));
    }
    let TableFactor::Table { name, alias, .. } = &twj.relation else {
        return Err(Error::Parse(
            "FROM must reference exactly one S3Object".into(),
        ));
    };
    if name.0.len() != 1 {
        return Err(Error::Parse(
            "FROM must reference exactly one S3Object".into(),
        ));
    }
    let sqlparser::ast::ObjectNamePart::Identifier(name_ident) = &name.0[0] else {
        // sqlparser 0.62 added `ObjectNamePart::Function` (a call in
        // object-name position) — never a valid FROM factor here.
        return Err(Error::Parse(
            "FROM must reference exactly one S3Object".into(),
        ));
    };
    if !name_ident.value.eq_ignore_ascii_case(S3_OBJECT) {
        return Err(Error::Parse(
            "FROM must reference exactly one S3Object".into(),
        ));
    }
    if alias.as_ref().map(|a| a.name.value.as_str()) != clause.alias.as_deref() {
        return Err(Error::Parse(
            "FROM must reference exactly one S3Object".into(),
        ));
    }
    Ok(())
}

/// The MISSING-rewrite sentinel function names: a user-authored call would
/// silently adopt MISSING semantics (review 2026-09-06b R15) — refused
/// before the rewrite, which itself inserts the names.
const SENTINEL_FUNCTIONS: [&str; 2] = ["__s3_is_missing", "__s3_is_not_missing"];

/// Reject a user-authored sentinel function call anywhere in the expression.
/// Runs on the ORIGINAL text (before `rewrite_is_missing` inserts the
/// sentinels) and is quote/comment-aware like every other scan: a `__s3_*`
/// name in call position (identifier followed by `(`) is refused with an
/// `Unsupported` — the request-level 400 path, never silent sentinel
/// adoption. The engine adds its own single-part-unquoted guard as defense.
fn reject_reserved_functions(sql: &str) -> Result<(), Error> {
    let bytes = sql.as_bytes();
    let mut state = ScanState::default();
    let mut i = 0;
    while i < bytes.len() {
        let (next, top) = state.step(bytes, i);
        if top {
            for sentinel in SENTINEL_FUNCTIONS {
                let end = i + sentinel.len();
                if bytes
                    .get(i..end)
                    .is_some_and(|w| w.eq_ignore_ascii_case(sentinel.as_bytes()))
                    && (i == 0 || !is_ident_byte(bytes[i - 1]))
                    && !bytes.get(end).is_some_and(|b| is_ident_byte(*b))
                    && function_paren(bytes, end)
                {
                    return Err(Error::Unsupported(format!(
                        "reserved function name: {sentinel}"
                    )));
                }
            }
        }
        i = next;
    }
    Ok(())
}

/// `(` after optional whitespace — the preceding word is in call position.
fn function_paren(bytes: &[u8], mut i: usize) -> bool {
    while bytes.get(i).is_some_and(|b| b.is_ascii_whitespace()) {
        i += 1;
    }
    bytes.get(i) == Some(&b'(')
}

/// Find the first top-level `FROM` keyword (quote-aware) and rewrite the
/// object clause: segments stripped, object text normalized to `S3Object`.
fn preprocess_from(sql: &str) -> Result<(FromClause, String), Error> {
    let bytes = sql.as_bytes();
    let Some((_, from_end)) = find_from_keyword(bytes) else {
        // No top-level FROM: leave the text for sqlparser; the validator
        // rejects (UNION, SELECT-without-FROM).
        return Ok((
            FromClause {
                segments: Vec::new(),
                alias: None,
            },
            sql.to_string(),
        ));
    };
    let mut j = from_end;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    // The object clause is self-terminating (its grammar binds segments
    // before any clause keyword), so parse the full remainder directly;
    // only when it is not an S3Object path do we need a roomy FROM-clause
    // scan to separate JOIN (unsupported) from a malformed object.
    let (clause, seg_end) = match parse_object_clause(&sql[j..]) {
        Ok(parsed) => parsed,
        Err(e) => {
            let clause_end = from_clause_end(bytes, j);
            if clause_has_join(bytes, j, clause_end) {
                return Err(Error::Unsupported("JOIN".into()));
            }
            return Err(e);
        }
    };
    let rewritten = format!("{}{}{}", &sql[..j], S3_OBJECT, &sql[j + seg_end..]);
    Ok((clause, rewritten))
}

/// Shared byte-scan state for the FROM scanners: quotes (`'`/`"` with
/// doubled-quote escapes) and comments (`--` line, `/* */` block) are
/// invisible to the word lookups — a comment or string containing `from`,
/// `join` or a clause keyword can never misdirect the FROM scan.
#[derive(Default)]
struct ScanState {
    in_string: bool,
    in_ident: bool,
    in_block: bool,
}

impl ScanState {
    /// Advance the scanner over one byte at `i`; returns the next index to
    /// examine and whether `i` was a plain top-level byte (outside quotes
    /// and comments — a word start may sit there).
    fn step(&mut self, bytes: &[u8], i: usize) -> (usize, bool) {
        if self.in_block {
            if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                self.in_block = false;
                (i + 2, false)
            } else {
                (i + 1, false)
            }
        } else if self.in_string {
            if bytes[i] == b'\'' && bytes.get(i + 1) == Some(&b'\'') {
                (i + 2, false) // doubled quote inside a string
            } else {
                let closing = bytes[i] == b'\'';
                if closing {
                    self.in_string = false;
                }
                (i + 1, false)
            }
        } else if self.in_ident {
            if bytes[i] == b'"' && bytes.get(i + 1) == Some(&b'"') {
                (i + 2, false)
            } else {
                let closing = bytes[i] == b'"';
                if closing {
                    self.in_ident = false;
                }
                (i + 1, false)
            }
        } else {
            match bytes[i] {
                b'\'' => {
                    self.in_string = true;
                    (i + 1, false)
                }
                b'"' => {
                    self.in_ident = true;
                    (i + 1, false)
                }
                b'-' if bytes.get(i + 1) == Some(&b'-') => {
                    // Line comment: everything to the newline, or the end.
                    let mut j = i + 2;
                    while j < bytes.len() && bytes[j] != b'\n' {
                        j += 1;
                    }
                    if j < bytes.len() {
                        (j + 1, false)
                    } else {
                        (j, false)
                    }
                }
                b'/' if bytes.get(i + 1) == Some(&b'*') => {
                    self.in_block = true;
                    (i + 2, false)
                }
                _ => (i + 1, true),
            }
        }
    }
}

/// Scan for the first top-level `FROM` keyword — quote- and comment-aware
/// (a `-- from` comment before the real FROM never misdirects the scan).
fn find_from_keyword(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut state = ScanState::default();
    let mut i = 0;
    while i < bytes.len() {
        let (next, plain) = state.step(bytes, i);
        if plain && (bytes[i] == b'f' || bytes[i] == b'F') && word_at(bytes, i, "from") {
            return Some((i, i + 4));
        }
        i = next;
    }
    None
}

/// End of the FROM clause: first top-level clause keyword or `;`.
fn from_clause_end(bytes: &[u8], start: usize) -> usize {
    let mut state = ScanState::default();
    let mut i = start;
    let mut candidate = Vec::new();
    while i < bytes.len() {
        let (next, plain) = state.step(bytes, i);
        if plain {
            if bytes[i] == b';' {
                return i;
            }
            if is_ascii_ident_start(bytes[i]) {
                candidate.clear();
                let mut k = i;
                while k < bytes.len() && is_ident_byte(bytes[k]) {
                    candidate.push(bytes[k]);
                    k += 1;
                }
                let word = std::str::from_utf8(&candidate).unwrap_or_default();
                if [
                    "where",
                    "group",
                    "order",
                    "having",
                    "limit",
                    "union",
                    "except",
                    "intersect",
                ]
                .iter()
                .any(|k| k.eq_ignore_ascii_case(word))
                {
                    return i;
                }
                i = k;
                continue;
            }
        }
        i = next;
    }
    bytes.len()
}

/// Join detection inside the FROM clause (the whole clause is rejected up
/// front so `FROM a JOIN b` reports JOIN rather than a path error).
fn clause_has_join(bytes: &[u8], start: usize, end: usize) -> bool {
    let mut state = ScanState::default();
    let mut i = start;
    while i < end {
        let (next, plain) = state.step(bytes, i);
        if plain && word_at(bytes, i, "join") {
            return true;
        }
        // A quote/comment span may run past `end` — clamp and stop.
        i = next.min(end);
    }
    false
}

/// `S3Object` + zero or more segments (`.` name, `*`, `[N]`, `[*]`, `['n']`)
/// and an optional `AS? alias`; returns the clause and the byte length of
/// the segment text (the splice point, the alias stays in the source).
fn parse_object_clause(s: &str) -> Result<(FromClause, usize), Error> {
    let bytes = s.as_bytes();
    let starts = bytes
        .get(..S3_OBJECT.len())
        .is_some_and(|b| b.eq_ignore_ascii_case(S3_OBJECT.as_bytes()));
    if !starts
        || bytes
            .get(S3_OBJECT.len())
            .copied()
            .is_some_and(is_ident_byte)
    {
        return Err(Error::Parse("invalid FROM: expected S3Object".into()));
    }
    let mut i = S3_OBJECT.len();
    let mut segments = Vec::new();
    while let Some(&b) = bytes.get(i) {
        match b {
            b'.' if !segments.is_empty() => {
                i += 1;
                match bytes.get(i) {
                    Some(b'*') => {
                        segments.push(PathSeg::Wild);
                        i += 1;
                    }
                    Some(_) => {
                        let start = i;
                        while bytes.get(i).copied().is_some_and(is_ident_byte) {
                            i += 1;
                        }
                        if i == start {
                            return Err(Error::Parse("invalid FROM path".into()));
                        }
                        segments.push(PathSeg::Name(s[start..i].to_string()));
                    }
                    None => return Err(Error::Parse("invalid FROM path".into())),
                }
            }
            b'.' => {
                return Err(Error::Parse(
                    "invalid FROM path: must start with S3Object[*]".into(),
                ));
            }
            b'[' if segments.is_empty() && bytes.get(i + 1) != Some(&b'*') => {
                return Err(Error::Parse(
                    "invalid FROM path: must start with S3Object[*]".into(),
                ));
            }
            b'[' => {
                i += 1;
                match bytes.get(i) {
                    Some(b'*') => {
                        i += 1;
                        expect(bytes, i, b']')?;
                        i += 1;
                        segments.push(PathSeg::Wild);
                    }
                    Some(n) if n.is_ascii_digit() => {
                        let start = i;
                        while bytes.get(i).copied().is_some_and(|n| n.is_ascii_digit()) {
                            i += 1;
                        }
                        let value = s[start..i]
                            .parse::<usize>()
                            .map_err(|_| Error::Parse("invalid FROM path".into()))?;
                        expect(bytes, i, b']')?;
                        i += 1;
                        segments.push(PathSeg::Index(value));
                    }
                    Some(b'\'') => {
                        i += 1;
                        let start = i;
                        while bytes.get(i) != Some(&b'\'') {
                            i += 1;
                            if i >= bytes.len() {
                                return Err(Error::Parse("invalid FROM path".into()));
                            }
                        }
                        let name = s[start..i].to_string();
                        expect(bytes, i + 1, b']')?;
                        i += 2;
                        segments.push(PathSeg::Name(name));
                    }
                    _ => return Err(Error::Parse("invalid FROM path".into())),
                }
            }
            _ => break,
        }
    }
    let seg_end = i;
    let mut alias = None;
    let mut j = i;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if word_at(bytes, j, "as") {
        j += 2;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let w =
            take_ident(bytes, &mut j).ok_or_else(|| Error::Parse("invalid FROM path".into()))?;
        if clause_keyword(&w) {
            return Err(Error::Parse("invalid FROM path".into()));
        }
        alias = Some(w);
    } else if let Some(w) = take_ident(bytes, &mut j).filter(|w| !clause_keyword(w)) {
        alias = Some(w);
    }
    Ok((FromClause { segments, alias }, seg_end))
}

/// Check whether a bare-word candidate actually starts a clause keyword
/// (then it is not an alias).
fn clause_keyword(s: &str) -> bool {
    CLAUSE_KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(s))
}

/// Consume an identifier (bare or `"quoted"`) at position `*i`.
fn take_ident(bytes: &[u8], i: &mut usize) -> Option<String> {
    match bytes.get(*i) {
        Some(b'"') => {
            let start = *i + 1;
            let mut k = start;
            while k < bytes.len() && bytes[k] != b'"' {
                k += 1;
            }
            if k >= bytes.len() {
                return None;
            }
            *i = k + 1;
            Some(String::from_utf8_lossy(&bytes[start..k]).into_owned())
        }
        Some(b) if is_ident_byte(*b) => {
            let start = *i;
            let mut k = start;
            while k < bytes.len() && is_ident_byte(bytes[k]) {
                k += 1;
            }
            *i = k;
            Some(String::from_utf8_lossy(&bytes[start..k]).into_owned())
        }
        _ => None,
    }
}

fn expect(bytes: &[u8], i: usize, b: u8) -> Result<(), Error> {
    if bytes.get(i) == Some(&b) {
        Ok(())
    } else {
        Err(Error::Parse("invalid FROM path".into()))
    }
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn is_ascii_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// Case-insensitive keyword match: the word must sit at statement position
/// (start, whitespace, `(`, `,` or `;`) — never after `.`/identifier-bytes,
/// which are field names like `s.from`.
fn word_at(bytes: &[u8], i: usize, word: &str) -> bool {
    let end = i + word.len();
    if end > bytes.len() || !bytes[i..end].eq_ignore_ascii_case(word.as_bytes()) {
        return false;
    }
    let boundary_before = i == 0
        || matches!(
            bytes[i - 1],
            b' ' | b'\t' | b'\r' | b'\n' | b'(' | b',' | b';'
        );
    boundary_before && (end == bytes.len() || !is_ident_byte(bytes[end]))
}

/// Rewrite every top-level `X IS [NOT] MISSING` into the sentinel calls
/// `__s3_is_missing(X)` / `__s3_is_not_missing(X)` — sqlparser 0.62 has no
/// `IsMissing` variant, and `IS NULL` substitution would be wrong (a
/// present-but-null field must not satisfy MISSING). Token-based, so
/// strings, quoted identifiers and comments are never entered. A chained
/// `X IS [NOT] MISSING IS MISSING` is not valid SQL: the second operand's
/// walk-back runs past the first rewrite's span — refused (Parse) rather
/// than splicing an overlapping range (an index-out-of-range panic on
/// untrusted request input).
fn rewrite_is_missing(sql: &str) -> Result<String, Error> {
    let mut tokenizer = Tokenizer::new(&GenericDialect, sql);
    let Ok(tokens) = tokenizer.tokenize_with_location() else {
        return Ok(sql.to_string()); // unterminated literal etc: let the parser report
    };
    let pos = positions(sql);
    let mut found: Vec<(usize, usize, usize, bool)> = Vec::new();
    let mut prev_missing_end = 0;
    let mut i = 0;
    // `positions` carries every char boundary, so a lookup can only miss on
    // a tokenizer/offset divergence — refuse instead of indexing the
    // HashMap (same panic-on-untrusted-input shape as the splice overlap).
    let offset = |t: &TokenWithSpan| -> Result<(usize, usize), Error> {
        let start = pos
            .get(&token_start(t))
            .copied()
            .ok_or_else(|| Error::Parse("invalid IS MISSING expression".into()))?;
        let end = pos
            .get(&token_end(t))
            .copied()
            .ok_or_else(|| Error::Parse("invalid IS MISSING expression".into()))?;
        Ok((start, end))
    };
    while i < tokens.len() {
        if !is_word(&tokens[i], "is") {
            i += 1;
            continue;
        }
        let mut j = skip_whitespace(&tokens, i + 1);
        let negated = tokens.get(j).is_some_and(|t| is_word(t, "not"));
        if negated {
            j = skip_whitespace(&tokens, j + 1);
        }
        if !tokens.get(j).is_some_and(|t| is_word(t, "missing")) {
            i += 1;
            continue;
        }
        // Walk back over the operand, tracking matching parens; stop at a
        // top-level token that binds looser than `IS` (a boundary keyword
        // after a `.` is a field name, e.g. `s.from`, not the keyword).
        let mut depth = 0usize;
        let mut k = i as isize - 1;
        while k >= 0 {
            match &tokens[k as usize].token {
                Token::RParen => depth += 1,
                Token::LParen => {
                    if depth > 0 {
                        depth -= 1;
                    } else {
                        break;
                    }
                }
                Token::Comma | Token::SemiColon if depth == 0 => break,
                Token::Word(w)
                    if depth == 0
                        && !(k > 0
                            && matches!(
                                tokens.get((k - 1) as usize).map(|t| &t.token),
                                Some(Token::Period)
                            ))
                        && OPERAND_BOUNDARY_KEYWORDS
                            .iter()
                            .any(|kw| w.value.eq_ignore_ascii_case(kw)) =>
                {
                    break;
                }
                _ => {}
            }
            k -= 1;
        }
        // skip whitespace after the boundary token so the splice keeps the
        // gap that preceded the operand
        let mut operand_token = (k + 1) as usize;
        while matches!(
            tokens.get(operand_token).map(|t| &t.token),
            Some(Token::Whitespace(_))
        ) {
            operand_token += 1;
        }
        let (operand_start, _) = offset(&tokens[operand_token])?;
        let (is_start, _) = offset(&tokens[i])?;
        let (_, missing_end) = offset(&tokens[j])?;
        if operand_start < prev_missing_end {
            // `x IS [NOT] MISSING IS MISSING` — the walk-back ran past the
            // previous match's span; the chained form is invalid SQL.
            return Err(Error::Parse("invalid IS MISSING expression".into()));
        }
        found.push((operand_start, is_start, missing_end, negated));
        prev_missing_end = missing_end;
        i = j + 1;
    }
    if found.is_empty() {
        return Ok(sql.to_string());
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for (operand_start, is_start, missing_end, negated) in found {
        out.push_str(&sql[last..operand_start]);
        out.push_str(if negated {
            "__s3_is_not_missing("
        } else {
            "__s3_is_missing("
        });
        out.push_str(&sql[operand_start..is_start]);
        out.push(')');
        last = missing_end;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

fn is_word(t: &sqlparser::tokenizer::TokenWithSpan, word: &str) -> bool {
    match &t.token {
        Token::Word(w) if w.quote_style.is_none() => w.value.eq_ignore_ascii_case(word),
        _ => false,
    }
}

fn skip_whitespace(tokens: &[sqlparser::tokenizer::TokenWithSpan], mut i: usize) -> usize {
    while matches!(tokens.get(i).map(|t| &t.token), Some(Token::Whitespace(_))) {
        i += 1;
    }
    i
}

fn token_start(t: &sqlparser::tokenizer::TokenWithSpan) -> (u64, u64) {
    (t.span.start.line, t.span.start.column)
}

fn token_end(t: &sqlparser::tokenizer::TokenWithSpan) -> (u64, u64) {
    (t.span.end.line, t.span.end.column)
}

/// (line, column) -> byte offset for every char boundary.
fn positions(sql: &str) -> HashMap<(u64, u64), usize> {
    let mut map = HashMap::new();
    let mut line = 1u64;
    let mut col = 1u64;
    let mut offset = 0usize;
    map.insert((line, col), offset);
    for c in sql.chars() {
        offset += c.len_utf8();
        if c == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
        map.insert((line, col), offset);
    }
    map
}

/// Does the select/list expression tree contain one of the five aggregate
/// functions (`count`/`sum`/`avg`/`min`/`max`, case-insensitive)?
/// Shared with the engine (Task 7): an aggregate nested inside another
/// aggregate's argument is rejected in both layers.
pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
    /// One aggregate-named `Function` anywhere in the tree — a visitor
    /// descends into every expression position structurally, so a sqlparser
    /// variant this file doesn't enumerate can never silently hide an
    /// aggregate (the old hand-rolled walker's `_ => false` did; review
    /// 2026-09-06 simplify).
    struct HasAggregate(bool);

    impl Visitor for HasAggregate {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if matches!(expr, Expr::Function(f) if is_aggregate_name(&f.name)) {
                self.0 = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let mut guard = HasAggregate(false);
    let _ = expr.visit(&mut guard);
    guard.0
}

fn is_aggregate_name(name: &sqlparser::ast::ObjectName) -> bool {
    let [sqlparser::ast::ObjectNamePart::Identifier(part)] = name.0.as_slice() else {
        return false;
    };
    ["count", "sum", "avg", "min", "max"]
        .iter()
        .any(|k| part.value.eq_ignore_ascii_case(k))
}

/// In aggregate mode every projection item must be one bare aggregate call
/// (Task 7): `count(*)`, `count(expr)`, `sum/avg/min/max(expr)` — no
/// DISTINCT, no FILTER/OVER/clauses, no wrapping (`count(*) + 1`). A
/// non-grouped column or an expression over an aggregate is invalid SQL
/// without GROUP BY (AWS rejects both), so the whole query is a request-
/// level parse error. `DISTINCT` gets its own message per the plan.
fn validate_aggregate_item(expr: &Expr) -> Result<(), Error> {
    let not_aggregate = || Error::Parse("non-aggregate expression in aggregate select list".into());
    let Expr::Function(f) = expr else {
        return Err(not_aggregate());
    };
    if !is_aggregate_name(&f.name)
        || f.parameters != FunctionArguments::None
        || f.filter.is_some()
        || f.over.is_some()
        || f.null_treatment.is_some()
        || !f.within_group.is_empty()
    {
        return Err(not_aggregate());
    }
    let FunctionArguments::List(list) = &f.args else {
        return Err(not_aggregate());
    };
    if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
        return Err(Error::Parse("distinct not supported".into()));
    }
    if !list.clauses.is_empty() {
        return Err(not_aggregate());
    }
    let [sqlparser::ast::ObjectNamePart::Identifier(part)] = f.name.0.as_slice() else {
        return Err(not_aggregate());
    };
    let name = part.value.to_ascii_lowercase();
    match (name.as_str(), list.args.as_slice()) {
        ("count", [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]) => Ok(()),
        (_, [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) if contains_aggregate(e) => {
            // An aggregate nested in the argument (`count(sum(s._1))`) is
            // not a bare call — refused here, so the engine's internal
            // guard can never surface for a user-constructed query.
            Err(not_aggregate())
        }
        (_, [FunctionArg::Unnamed(FunctionArgExpr::Expr(_))]) => Ok(()),
        _ => Err(not_aggregate()),
    }
}

/// Parquet projection set (Task 11): the column names referenced by SELECT
/// items + WHERE — pruning input for the parquet reader's projection mask.
/// Grilling Q6 — JSON output column naming: a projection names its JSON
/// key from the alias, else (a plain field reference only) from the field
/// name; any other expression without an alias (`s.x + 1`, `count(*)`)
/// would need a guessed key, which the server rejects at request level
/// (400 `InvalidRequestParameter`) for JSON output. CSV output is
/// positional and never gated. The predicate pins the exemption — it
/// mirrors `engine::plain_key`'s field-reference shapes.
pub fn projection_needs_alias(item: &Projection) -> bool {
    match item {
        Projection::Wild => false,
        Projection::Item { alias: Some(_), .. } => false,
        Projection::Item { expr, alias: None } => !matches!(
            expr,
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::CompoundFieldAccess { .. }
        ),
    }
}

/// Flat-record semantics mirror `eval_field` (single identifier → its
/// column; compound identifier → the last part, `parts[0]` is the FROM
/// alias). A `Wild` projection reads everything — the empty set, which the
/// reader treats as "all columns". Names pass as written; the reader
/// matches them against the schema case-insensitively (identifier rules).
pub fn referenced_columns(plan: &QueryPlan) -> Vec<String> {
    if plan
        .projections
        .iter()
        .any(|p| matches!(p, Projection::Wild))
    {
        return Vec::new();
    }
    let mut names = Vec::new();
    let mut collect = Columns(&mut names);
    for item in &plan.projections {
        let Projection::Item { expr, .. } = item else {
            unreachable!("Wild rejected above");
        };
        let _ = expr.visit(&mut collect);
    }
    if let Some(expr) = &plan.where_expr {
        let _ = expr.visit(&mut collect);
    }
    names.sort_by_key(|a| a.to_lowercase());
    names.dedup();
    names
}

/// Column-name collector: a plain identifier names its column; a compound
/// identifier (dot chain, `s.a.b` included — sqlparser folds a plain
/// identifier chain into `CompoundIdentifier`) names its last part. Only
/// bracket/subscript access chains (`s.projects[0].name`, the
/// `CompoundFieldAccess` shape) are not flat-record column references (the
/// engine resolves them as a literal name that matches none) — omitted
/// rather than guessed.
struct Columns<'a>(&'a mut Vec<String>);

impl Visitor for Columns<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => self.0.push(id.value.clone()),
            Expr::CompoundIdentifier(parts) => self.0.push(
                parts
                    .last()
                    .expect("compound identifier is non-empty")
                    .value
                    .clone(),
            ),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Expression-position subqueries: `WHERE x IN (SELECT …)`, `EXISTS (…)`, a
/// bare `(SELECT …)` — the top-level `SetExpr::Query` arm catches only the
/// statement shape, so these would otherwise validate and then fail in the
/// eval catch-all in-stream (review 2026-09-06b R4). Refused here,
/// request-level — the same `Unsupported("subquery")` surface.
fn reject_subqueries(expr: &Expr) -> Result<(), Error> {
    struct Guard(bool);
    impl Visitor for Guard {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if matches!(
                expr,
                Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }
            ) {
                self.0 = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let mut guard = Guard(false);
    let _ = expr.visit(&mut guard);
    if guard.0 {
        return Err(Error::Unsupported("subquery".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{
        AccessExpr, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Subscript,
    };

    use super::*;

    fn ok(sql: &str) -> QueryPlan {
        parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    #[track_caller]
    fn rej(sql: &str, contains: &str) {
        match parse(sql) {
            Err(e) => {
                let s = e.to_string();
                assert!(
                    s.contains(contains),
                    "{sql}: {s} does not contain {contains:?}"
                );
            }
            Ok(_) => panic!("{sql}: expected rejection containing {contains:?}"),
        }
    }

    fn name(expr: &Expr) -> &str {
        match expr {
            Expr::Identifier(i) => i.value.as_str(),
            _ => panic!("expected identifier, got {expr}"),
        }
    }

    fn item(plan: &QueryPlan, i: usize) -> &Expr {
        match &plan.projections[i] {
            Projection::Item { expr, .. } => expr,
            Projection::Wild => panic!("projection {i} is wild"),
        }
    }

    #[test]
    fn wildcard_where_limit() {
        let q = ok("SELECT * FROM S3Object s WHERE s._3 > 100 LIMIT 5");
        assert!(q.from.segments.is_empty());
        assert_eq!(q.from.alias.as_deref(), Some("s"));
        assert_eq!(q.projections, vec![Projection::Wild]);
        assert!(matches!(q.where_expr, Some(Expr::BinaryOp { .. })));
        assert_eq!(q.limit, Some(5));
        assert!(!q.aggregates);
    }

    #[test]
    fn compound_identifier_projection() {
        let q = ok("SELECT s.Id, s.Name AS n FROM S3Object s");
        assert_eq!(q.projections.len(), 2);
        match (item(&q, 0), &q.projections[1]) {
            (Expr::CompoundIdentifier(parts), Projection::Item { expr, alias }) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0].value, "s");
                assert_eq!(parts[1].value, "Id");
                assert_eq!(alias.as_deref(), Some("n"));
                match expr {
                    Expr::CompoundIdentifier(parts) => assert_eq!(parts[1].value, "Name"),
                    other => panic!("expected compound identifier, got {other}"),
                }
            }
            (other, _) => panic!("expected compound identifier, got {other}"),
        }
    }

    #[test]
    fn traversal_path() {
        let q = ok("SELECT price FROM S3Object[*].books[*].price");
        assert_eq!(
            q.from.segments,
            vec![
                PathSeg::Wild,
                PathSeg::Name("books".into()),
                PathSeg::Wild,
                PathSeg::Name("price".into()),
            ]
        );
        assert_eq!(q.from.alias, None);
        assert_eq!(name(item(&q, 0)), "price");
    }

    #[test]
    fn bracket_subscript_access() {
        let q = ok("SELECT s.projects[0].project_name FROM S3Object s");
        match item(&q, 0) {
            Expr::CompoundFieldAccess { root, access_chain } => {
                assert_eq!(name(root), "s");
                assert_eq!(access_chain.len(), 3);
                assert!(matches!(access_chain[0], AccessExpr::Dot(_)));
                match &access_chain[1] {
                    AccessExpr::Subscript(Subscript::Index { index }) => {
                        assert_eq!(index.to_string(), "0");
                    }
                    other => panic!("expected index subscript, got {other}"),
                }
                assert!(matches!(access_chain[2], AccessExpr::Dot(_)));
            }
            other => panic!("expected compound field access, got {other}"),
        }
    }

    #[test]
    fn count_star_is_aggregate() {
        let q = ok("SELECT count(*) FROM S3Object s");
        assert!(q.aggregates);
        assert_eq!(q.projections.len(), 1);
        match item(&q, 0) {
            Expr::Function(f) => {
                assert_eq!(f.name.to_string(), "count");
                let wildcard = match &f.args {
                    FunctionArguments::List(list) => list
                        .args
                        .iter()
                        .any(|a| matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))),
                    _ => false,
                };
                assert!(wildcard, "count(*) must carry a wildcard arg");
            }
            other => panic!("expected function, got {other}"),
        }
    }

    #[test]
    fn bare_expression_without_alias() {
        let q = ok("SELECT s.x+1 FROM S3Object s");
        assert_eq!(q.projections.len(), 1);
        match (&q.projections[0], item(&q, 0)) {
            (Projection::Item { alias, .. }, Expr::BinaryOp { op, .. }) => {
                assert_eq!(alias, &None);
                assert_eq!(op.to_string(), "+");
            }
            (_, other) => panic!("expected binary op, got {other}"),
        }
    }

    #[test]
    fn reject_join() {
        rej("SELECT * FROM a JOIN b", "unsupported: JOIN");
    }

    #[test]
    fn reject_group_by() {
        rej("SELECT * FROM S3Object GROUP BY 1", "unsupported: GROUP BY");
    }

    #[test]
    fn reject_order_by() {
        rej("SELECT * FROM S3Object ORDER BY 1", "unsupported: ORDER BY");
    }

    #[test]
    fn reject_distinct() {
        rej("SELECT DISTINCT x FROM S3Object", "unsupported: DISTINCT");
    }

    #[test]
    fn reject_union() {
        rej("(SELECT 1) UNION (SELECT 2)", "unsupported: UNION");
    }

    #[test]
    fn reject_bare_path_first_segment() {
        rej("SELECT * FROM S3Object.name", "must start with S3Object[*]");
    }

    #[test]
    fn is_missing_after_non_ascii_text() {
        // R16 nit of the `positions()` char-vs-byte worry: sqlparser's
        // tokenizer columns are character-based (query.chars(), 0.57 and
        // 0.62 alike), and `positions()` counts chars too — a multi-byte
        // literal before the predicate must not misalign the IS MISSING
        // rewrite. Pinned.
        let plan = parse("SELECT * FROM S3Object s WHERE s.a = 'ü' OR s.b IS MISSING").unwrap();
        assert!(plan.where_expr.is_some());
        // The rewrite is not wrapped in an error path: a sentinel call must
        // be present in the rewritten expression (the engine walks it).
        let s = plan.where_expr.unwrap().to_string();
        assert!(s.contains("__s3_is_missing"), "{s}");
    }

    #[test]
    fn reject_expression_position_subqueries() {
        // R4: the top-level `SetExpr::Query` arm catches the statement shape
        // only — an `IN (SELECT …)`, `EXISTS` or a bare `(SELECT …)` in the
        // WHERE or the projection would otherwise validate and then die in
        // the eval catch-all in-stream. Request-level 400s now.
        rej(
            "SELECT s._1 FROM S3Object s WHERE s._1 IN (SELECT t.a FROM t)",
            "unsupported: subquery",
        );
        rej(
            "SELECT * FROM S3Object s WHERE EXISTS (SELECT 1)",
            "unsupported: subquery",
        );
        rej("SELECT (SELECT 1) FROM S3Object s", "unsupported: subquery");
        rej(
            "SELECT s._1 AS x FROM S3Object s WHERE s.a = (SELECT 1)",
            "unsupported: subquery",
        );
    }

    #[test]
    fn reject_reserved_sentinel_functions() {
        // R15: user-authored sentinel calls (unqualified, qualified or
        // quoted) would silently adopt MISSING semantics — refused at parse.
        rej(
            "SELECT __s3_is_missing(s.a) FROM S3Object s",
            "reserved function name: __s3_is_missing",
        );
        rej(
            "SELECT * FROM S3Object s WHERE foo.__s3_is_missing(s.a)",
            "reserved function name: __s3_is_missing",
        );
        rej(
            "SELECT * FROM S3Object s WHERE s.a IS MISSING OR foo.__s3_is_not_missing(s.b)",
            "reserved function name: __s3_is_not_missing",
        );
    }

    #[test]
    fn reject_aggregates_plus_wild() {
        rej(
            "SELECT *, count(*) FROM S3Object s",
            "aggregates require an explicit select list",
        );
    }

    #[test]
    fn reject_mixed_aggregate_list() {
        // A non-grouped column next to an aggregate is invalid SQL — the
        // list must be bare aggregate calls only (AWS rejects it too).
        rej(
            "SELECT s._1, count(*) FROM S3Object s",
            "non-aggregate expression in aggregate select list",
        );
    }

    #[test]
    fn reject_wrapped_aggregate() {
        // An expression over an aggregate (count(*) + 1) is not a bare
        // aggregate call: rejected at parse like the mixed list.
        rej(
            "SELECT count(*) + 1 FROM S3Object s",
            "non-aggregate expression in aggregate select list",
        );
    }

    #[test]
    fn reject_nested_aggregate_arg() {
        // An aggregate nested in an aggregate's argument is still not a bare
        // call — the arg carrying its own aggregate is the same invalid
        // shape, refused at parse (never leaking an internal guard).
        rej(
            "SELECT count(sum(s._1)) FROM S3Object s",
            "non-aggregate expression in aggregate select list",
        );
    }

    #[test]
    fn reject_aggregate_distinct() {
        rej(
            "SELECT count(DISTINCT s._1) FROM S3Object s",
            "distinct not supported",
        );
    }

    #[test]
    fn aggregates_in_where_are_parse_errors() {
        // The engine's aggregate channel derives from projections only
        // (`plan.aggregates`); a WHERE aggregate would fail in-stream as an
        // internal error — refused request-level.
        rej(
            "SELECT s.a FROM S3Object s WHERE count(*) > 1",
            "aggregates not allowed in WHERE",
        );
        rej(
            "SELECT s.a FROM S3Object s WHERE sum(s.x) > 0",
            "aggregates not allowed in WHERE",
        );
    }

    #[test]
    fn aggregate_bare_forms_parse() {
        let q = ok(
            "SELECT count(*), count(s._1) AS c, sum(s._2), avg(s._2), min(s._2), max(s._2) \
             FROM S3Object s",
        );
        assert!(q.aggregates);
        assert_eq!(q.projections.len(), 6);
    }

    #[test]
    fn is_missing_rewrite() {
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS MISSING");
        match q.where_expr {
            Some(Expr::Function(f)) => {
                assert_eq!(f.name.to_string(), "__s3_is_missing");
            }
            other => panic!("expected missing sentinel, got {other:?}"),
        }
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING");
        match q.where_expr {
            Some(Expr::Function(f)) => {
                assert_eq!(f.name.to_string(), "__s3_is_not_missing");
            }
            other => panic!("expected not-missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn is_missing_rewrite_quote_aware() {
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x = 'x IS MISSING y'");
        assert!(matches!(q.where_expr, Some(Expr::BinaryOp { .. })));
    }

    #[test]
    fn is_missing_chained_forms_are_parse_errors() {
        // `x IS MISSING IS MISSING`: the second operand's walk-back would
        // overlap the first match's span — a splice panic (index out of
        // range) on untrusted input; the chained form is invalid SQL.
        rej(
            "SELECT s.x FROM S3Object s WHERE s.x IS MISSING IS MISSING",
            "invalid IS MISSING expression",
        );
        rej(
            "SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING IS MISSING",
            "invalid IS MISSING expression",
        );
        // The legit single form still rewrites.
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS MISSING");
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), "__s3_is_missing"),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn traversal_plus_missing_rewrite() {
        let q = ok("SELECT s.x FROM S3Object[*].books[*].price s WHERE s.x IS MISSING");
        assert_eq!(q.from.segments.last(), Some(&PathSeg::Name("price".into())));
        match q.where_expr {
            Some(Expr::Function(f)) => {
                assert_eq!(f.name.to_string(), "__s3_is_missing");
            }
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn limit_must_be_positive() {
        rej(
            "SELECT * FROM S3Object LIMIT -5",
            "LIMIT must be a positive integer",
        );
        rej(
            "SELECT * FROM S3Object LIMIT 0",
            "LIMIT must be a positive integer",
        );
    }

    #[test]
    fn expression_too_long() {
        let sql = format!(
            "SELECT s.x FROM S3Object s WHERE s.x = '{}'",
            "a".repeat(300 * 1024)
        );
        rej(&sql, "expression exceeds 256 KiB");
    }

    #[test]
    fn keyword_named_fields_are_not_keywords() {
        // A path segment spelled like a clause keyword stays a segment.
        let q = ok("SELECT x FROM S3Object[*].limit.name s");
        assert_eq!(
            q.from.segments,
            vec![
                PathSeg::Wild,
                PathSeg::Name("limit".into()),
                PathSeg::Name("name".into())
            ]
        );
        // A field named `from` is a field, and its MISSING operand is exact.
        let q = ok("SELECT s.from FROM S3Object s WHERE s.from IS MISSING");
        assert_eq!(q.projections.len(), 1);
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), "__s3_is_missing"),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn from_scan_is_comment_aware() {
        // A `-- from` line comment before the real FROM must not misdirect
        // the byte scanner (the comment-blind scan parsed the comment's
        // `from` as the FROM clause and reported a bogus path error).
        let q = ok("SELECT x -- from table\nFROM S3Object s");
        assert_eq!(q.projections.len(), 1);
        // A newline inside a block comment puts `from` at a word boundary —
        // the comment-blind scanner matched there too.
        let q = ok("SELECT x /*\nfrom\n*/ FROM S3Object s");
        assert_eq!(q.projections.len(), 1);
        // A JOIN word inside a comment in a non-S3Object FROM clause is not
        // a JOIN — the path error stands, never the JOIN channel.
        rej(
            "SELECT x FROM other -- join\nx",
            "invalid FROM: expected S3Object",
        );
    }

    #[test]
    fn bare_column_from_before_from_is_a_parse_error() {
        // A bare column named `from` is a reserved word — the byte scanner
        // cannot distinguish it from the real FROM (full tokenization would
        // be needed); the honest outcome is the same Parse-error family,
        // never a panic or an Unsupported channel.
        rej(
            "SELECT from FROM S3Object s",
            "invalid FROM: expected S3Object",
        );
    }

    // ------------------------------------------------------------------
    // Parquet projection set (Task 11).
    // ------------------------------------------------------------------

    #[test]
    fn referenced_columns_union_of_select_and_where() {
        let q = ok("SELECT s.a, s.b2 FROM S3Object s WHERE s.c > 1");
        assert_eq!(referenced_columns(&q), vec!["a", "b2", "c"]);
        let q = ok("SELECT s.a FROM S3Object s WHERE s.c = s.d");
        assert_eq!(referenced_columns(&q), vec!["a", "c", "d"]);
    }

    #[test]
    fn referenced_columns_wild_means_all_columns() {
        // SELECT * reads every schema column regardless of WHERE refs.
        let q = ok("SELECT * FROM S3Object s WHERE s.a > 1");
        assert_eq!(referenced_columns(&q), Vec::<String>::new());
        let q = ok("SELECT * FROM S3Object");
        assert_eq!(referenced_columns(&q), Vec::<String>::new());
    }

    #[test]
    fn referenced_columns_walks_expression_children() {
        // Aggregates, the MISSING sentinel, arithmetic, BETWEEN and LIKE:
        // the walker descends into every child expression.
        let q = ok("SELECT count(s._1), sum(s.x) FROM S3Object s WHERE s.y IS MISSING");
        assert_eq!(referenced_columns(&q), vec!["_1", "x", "y"]);
        let q = ok("SELECT s.a + 1 FROM S3Object s WHERE s.b BETWEEN 1 AND 2");
        assert_eq!(referenced_columns(&q), vec!["a", "b"]);
        let q = ok("SELECT s.a FROM S3Object s WHERE s.b LIKE 'x%'");
        assert_eq!(referenced_columns(&q), vec!["a", "b"]);
        let q = ok("SELECT s.a FROM S3Object s WHERE s.d IN (1, 2)");
        assert_eq!(referenced_columns(&q), vec!["a", "d"]);
    }

    #[test]
    fn referenced_columns_keeps_identifier_folding() {
        // Names pass as written (the reader matches the schema
        // case-insensitively); quoted identifiers keep their exact case.
        let q = ok("SELECT s.ID FROM S3Object s");
        assert_eq!(referenced_columns(&q), vec!["ID"]);
        let q = ok("SELECT s.\"id\" FROM S3Object s WHERE s.NAME = 'x'");
        assert_eq!(referenced_columns(&q), vec!["id", "NAME"]);
    }

    // ------------------------------------------------------------------
    // Coverage round (2026-09-06): the rejection arms and traversal edges
    // below were not reached by the parser suite.
    // ------------------------------------------------------------------

    #[test]
    fn empty_expression_is_parse_error() {
        rej("", "empty expression");
    }

    #[test]
    fn multiple_statements_are_parse_errors() {
        rej(
            "SELECT s.a FROM S3Object s; SELECT s.b FROM S3Object s",
            "single statement",
        );
    }

    #[test]
    fn non_query_statement_rejected() {
        // `Statement::CreateTable` is not a `Query` — the top-level shape
        // arm refuses it before any FROM/validation work.
        rej("CREATE TABLE t (a INT)", "invalid statement");
    }

    #[test]
    fn reject_with_clause() {
        rej(
            "WITH s AS (SELECT 1) SELECT s.a FROM S3Object s",
            "unsupported: WITH",
        );
    }

    #[test]
    fn reject_group_by_all() {
        rej(
            "SELECT s.a FROM S3Object s GROUP BY ALL",
            "unsupported: GROUP BY",
        );
    }

    #[test]
    fn reject_having() {
        rej(
            "SELECT count(*) FROM S3Object s HAVING count(*) > 1",
            "unsupported: HAVING",
        );
    }

    #[test]
    fn reject_offset() {
        rej(
            "SELECT s.a FROM S3Object s LIMIT 5 OFFSET 2",
            "unsupported: OFFSET",
        );
    }

    #[test]
    fn reject_qualified_wildcard() {
        rej(
            "SELECT s.* FROM S3Object s",
            "unsupported: qualified wildcard",
        );
    }

    #[test]
    fn limit_must_be_integer_value() {
        // A non-integer literal (float) is not a positive integer.
        rej(
            "SELECT s.a FROM S3Object s LIMIT 1.5",
            "LIMIT must be a positive integer",
        );
    }

    #[test]
    fn reject_two_from_factors() {
        rej(
            "SELECT * FROM S3Object a, S3Object b",
            "FROM must reference exactly one S3Object",
        );
    }

    #[test]
    fn reject_join_on_valid_object() {
        // A valid single S3Object factor carrying a JOIN reaches the
        // validator's JOIN arm (the other JOIN path goes through the
        // prepass clause scan when the object clause itself is invalid).
        rej(
            "SELECT * FROM S3Object s JOIN t ON s.x = t.x",
            "unsupported: JOIN",
        );
    }

    #[test]
    fn quoted_ident_doubling_survives_scan() {
        // ScanState handles `"`/`'` doubling; a comment or quoted string
        // containing a `from` word must not misdirect the FROM preprocessor.
        let q = ok("SELECT s.\"a\"\"b\" FROM S3Object s");
        assert_eq!(q.projections.len(), 1);
        // Doubled single-quote escape inside a literal before `from`.
        let q = ok("SELECT s.a FROM S3Object s WHERE s.a = 'it''s'");
        assert!(matches!(q.where_expr, Some(Expr::BinaryOp { .. })));
    }

    #[test]
    fn missing_bracket_is_invalid_from_path() {
        rej("SELECT * FROM S3Object[*", "invalid FROM path");
        rej("SELECT * FROM S3Object[", "invalid FROM path");
    }

    #[test]
    fn index_overflow_is_invalid_from_path() {
        rej(
            "SELECT * FROM S3Object[*].books[99999999999999999999]",
            "invalid FROM path",
        );
    }

    #[test]
    fn unterminated_quoted_segment_is_invalid_from_path() {
        rej("SELECT * FROM S3Object[*].'books", "invalid FROM path");
    }

    #[test]
    fn referenced_columns_plain_identifier() {
        // A single-part (unqualified) column reference drives the Visitor's
        // bare-`Identifier` arm (the other tests use compounded refs).
        let q = ok("SELECT a, b FROM S3Object s");
        assert_eq!(referenced_columns(&q), vec!["a", "b"]);
    }

    #[test]
    fn projection_needs_alias_logic() {
        // Wild needs no alias; an explicit alias never does; a bare field
        // reference does not; any other expression does.
        let q = ok("SELECT * FROM S3Object s");
        assert!(!projection_needs_alias(&q.projections[0]));
        let q = ok("SELECT s.a AS x FROM S3Object s");
        assert!(!projection_needs_alias(&q.projections[0]));
        let q = ok("SELECT s.a FROM S3Object s");
        assert!(!projection_needs_alias(&q.projections[0]));
        let q = ok("SELECT s.a + 1 FROM S3Object s");
        assert!(projection_needs_alias(&q.projections[0]));
        let q = ok("SELECT count(*) FROM S3Object s");
        assert!(projection_needs_alias(&q.projections[0]));
    }
}
