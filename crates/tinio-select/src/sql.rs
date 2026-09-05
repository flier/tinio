//! SQL parse + validate: a FROM-path preprocessor, a quote-aware
//! `IS [NOT] MISSING` rewrite, then sqlparser + a restrict-grammar
//! validator producing the engine's `QueryPlan`.

use std::collections::HashMap;

use sqlparser::ast::{
    AccessExpr, CaseWhen, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, LimitClause, SelectItem, SetExpr, Statement, Subscript, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::SelectError;

/// One step of the FROM object path (`S3Object[*].books[0]`); `Index` is
/// 0-based.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSeg {
    Name(String),
    Index(usize),
    Wild,
}

/// The parsed FROM clause: the S3Object path walk plus the optional alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub traversed: bool,
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

/// Validated query plan consumed by the engine (`eval`, `JsonReader`).
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
    "where", "group", "order", "having", "limit", "union", "except", "intersect", "join",
    "left", "right", "inner", "cross", "full", "natural", "on", "using", "offset",
    "fetch", "window", "qualify", "settings", "format",
];

/// Keywords that terminate an operand walk-back in the `IS [NOT] MISSING`
/// rewrite: everything that binds looser than `IS`.
const OPERAND_BOUNDARY_KEYWORDS: &[&str] = &[
    "and", "or", "not", "where", "select", "from", "limit", "when", "then", "else",
];

/// Parse + validate one S3 Select expression.
pub fn parse(sql: &str) -> Result<QueryPlan, SelectError> {
    if sql.len() > MAX_EXPRESSION {
        return Err(SelectError::Parse("expression exceeds 256 KiB".into()));
    }
    let (from, rewritten) = preprocess_from(sql)?;
    let rewritten = rewrite_is_missing(&rewritten);
    let stmts = Parser::parse_sql(&GenericDialect, &rewritten)
        .map_err(|e| SelectError::Parse(format!("syntax error: {e}")))?;
    let stmt = match stmts.as_slice() {
        [s] => s,
        [] => return Err(SelectError::Parse("empty expression".into())),
        _ => return Err(SelectError::Parse("expression must be a single statement".into())),
    };
    let Statement::Query(query) = stmt else {
        return Err(SelectError::Parse(format!("invalid statement: {stmt}")));
    };
    if query.with.is_some() {
        return Err(SelectError::Unsupported("WITH".into()));
    }
    if query.order_by.is_some() {
        return Err(SelectError::Unsupported("ORDER BY".into()));
    }
    let select = match &*query.body {
        SetExpr::Select(s) => s.as_ref(),
        SetExpr::SetOperation { .. } => return Err(SelectError::Unsupported("UNION".into())),
        SetExpr::Query(_) => return Err(SelectError::Unsupported("subquery".into())),
        _ => return Err(SelectError::Parse(format!("unexpected statement: {}", query.body))),
    };
    if select.distinct.is_some() {
        return Err(SelectError::Unsupported("DISTINCT".into()));
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if !exprs.is_empty() || !modifiers.is_empty() => {
            return Err(SelectError::Unsupported("GROUP BY".into()));
        }
        GroupByExpr::All(_) => return Err(SelectError::Unsupported("GROUP BY".into())),
        GroupByExpr::Expressions(_, _) => {}
    }
    if select.having.is_some() {
        return Err(SelectError::Unsupported("HAVING".into()));
    }
    let limit = match &query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset { limit: Some(limit), offset, limit_by }) => {
            if offset.is_some() {
                return Err(SelectError::Unsupported("OFFSET".into()));
            }
            if !limit_by.is_empty() {
                return Err(SelectError::Unsupported("LIMIT BY".into()));
            }
            positive_limit(limit)?
        }
        Some(LimitClause::LimitOffset { limit: None, .. }) => {
            return Err(SelectError::Parse("LIMIT must be a positive integer".into()));
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            return Err(SelectError::Parse("LIMIT must be a positive integer".into()));
        }
    };
    validate_from(select, &from)?;
    let mut projections = Vec::with_capacity(select.projection.len());
    let mut aggregates = false;
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => projections.push(Projection::Wild),
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(SelectError::Unsupported("qualified wildcard".into()));
            }
            SelectItem::UnnamedExpr(expr) => {
                aggregates |= contains_aggregate(expr);
                projections.push(Projection::Item {
                    expr: expr.clone(),
                    alias: None,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                aggregates |= contains_aggregate(expr);
                projections.push(Projection::Item {
                    expr: expr.clone(),
                    alias: Some(alias.value.clone()),
                });
            }
        }
    }
    if aggregates && projections.iter().any(|p| matches!(p, Projection::Wild)) {
        return Err(SelectError::Parse("aggregates require an explicit select list".into()));
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
fn positive_limit(limit: &Expr) -> Result<Option<usize>, SelectError> {
    match limit {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => match n.parse::<usize>() {
                Ok(n) if n > 0 => Ok(Some(n)),
                _ => Err(SelectError::Parse("LIMIT must be a positive integer".into())),
            },
            _ => Err(SelectError::Parse("LIMIT must be a positive integer".into())),
        },
        _ => Err(SelectError::Parse("LIMIT must be a positive integer".into())),
    }
}

/// The FROM clause must be exactly the single `S3Object` factor (with the
/// alias the preprocessor recorded).
fn validate_from(select: &sqlparser::ast::Select, clause: &FromClause) -> Result<(), SelectError> {
    let twj = match select.from.as_slice() {
        [t] => t,
        _ => {
            return Err(SelectError::Parse(
                "FROM must reference exactly one S3Object".into(),
            ));
        }
    };
    if !twj.joins.is_empty() {
        return Err(SelectError::Unsupported("JOIN".into()));
    }
    let TableFactor::Table { name, alias, .. } = &twj.relation else {
        return Err(SelectError::Parse("FROM must reference exactly one S3Object".into()));
    };
    if name.0.len() != 1 {
        return Err(SelectError::Parse("FROM must reference exactly one S3Object".into()));
    }
    let sqlparser::ast::ObjectNamePart::Identifier(name_ident) = &name.0[0];
    if !name_ident.value.eq_ignore_ascii_case(S3_OBJECT) {
        return Err(SelectError::Parse("FROM must reference exactly one S3Object".into()));
    }
    if alias.as_ref().map(|a| a.name.value.as_str()) != clause.alias.as_deref() {
        return Err(SelectError::Parse("FROM must reference exactly one S3Object".into()));
    }
    Ok(())
}

/// Find the first top-level `FROM` keyword (quote-aware) and rewrite the
/// object clause: segments stripped, object text normalized to `S3Object`.
fn preprocess_from(sql: &str) -> Result<(FromClause, String), SelectError> {
    let bytes = sql.as_bytes();
    let Some((_, from_end)) = find_from_keyword(bytes) else {
        // No top-level FROM: leave the text for sqlparser; the validator
        // rejects (UNION, SELECT-without-FROM).
        return Ok((
            FromClause {
                traversed: false,
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
                return Err(SelectError::Unsupported("JOIN".into()));
            }
            return Err(e);
        }
    };
    let rewritten = format!("{}{}{}", &sql[..j], S3_OBJECT, &sql[j + seg_end..]);
    Ok((clause, rewritten))
}

/// Quote-aware scan for the first `FROM` keyword (byte scanner toggling on
/// `'` strings and `"` quoted identifiers).
fn find_from_keyword(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut in_string = false;
    let mut in_ident = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if in_string && bytes.get(i + 1) == Some(&b'\'') {
                    i += 1; // escaped quote inside a string
                } else {
                    in_string = !in_string;
                }
            }
            b'"' => {
                if in_ident && bytes.get(i + 1) == Some(&b'"') {
                    i += 1;
                } else {
                    in_ident = !in_ident;
                }
            }
            _ if !in_string && !in_ident && (bytes[i] == b'f' || bytes[i] == b'F')
                && word_at(bytes, i, "from") =>
            {
                return Some((i, i + 4));
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// End of the FROM clause: first top-level clause keyword or `;`.
fn from_clause_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    let mut in_string = false;
    let mut in_ident = false;
    let mut candidate = Vec::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if in_string && bytes.get(i + 1) == Some(&b'\'') {
                    i += 1;
                } else {
                    in_string = !in_string;
                }
            }
            b'"' => {
                if in_ident && bytes.get(i + 1) == Some(&b'"') {
                    i += 1;
                } else {
                    in_ident = !in_ident;
                }
            }
            b';' if !in_string && !in_ident => return i,
            _ if !in_string && !in_ident && is_ascii_ident_start(bytes[i]) => {
                candidate.clear();
                let mut k = i;
                while k < bytes.len() && is_ident_byte(bytes[k]) {
                    candidate.push(bytes[k]);
                    k += 1;
                }
                let word = std::str::from_utf8(&candidate).unwrap_or_default();
                if ["where", "group", "order", "having", "limit", "union", "except", "intersect"]
                    .iter()
                    .any(|k| k.eq_ignore_ascii_case(word))
                {
                    return i;
                }
                i = k;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    bytes.len()
}

/// Quote-aware JOIN detection inside the FROM clause (the whole clause is
/// rejected up front so `FROM a JOIN b` reports JOIN rather than a path
/// error).
fn clause_has_join(bytes: &[u8], start: usize, end: usize) -> bool {
    let mut i = start;
    let mut in_string = false;
    let mut in_ident = false;
    while i < end {
        match bytes[i] {
            b'\'' => {
                if in_string && bytes.get(i + 1) == Some(&b'\'') {
                    i += 1;
                } else {
                    in_string = !in_string;
                }
            }
            b'"' => {
                if in_ident && bytes.get(i + 1) == Some(&b'"') {
                    i += 1;
                } else {
                    in_ident = !in_ident;
                }
            }
            _ if !in_string && !in_ident && word_at(bytes, i, "join") => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

/// `S3Object` + zero or more segments (`.` name, `*`, `[N]`, `[*]`, `['n']`)
/// and an optional `AS? alias`; returns the clause and the byte length of
/// the segment text (the splice point, the alias stays in the source).
fn parse_object_clause(s: &str) -> Result<(FromClause, usize), SelectError> {
    let bytes = s.as_bytes();
    let starts = bytes
        .get(..S3_OBJECT.len())
        .is_some_and(|b| b.eq_ignore_ascii_case(S3_OBJECT.as_bytes()));
    if !starts || bytes.get(S3_OBJECT.len()).copied().is_some_and(is_ident_byte) {
        return Err(SelectError::Parse("invalid FROM: expected S3Object".into()));
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
                            return Err(SelectError::Parse("invalid FROM path".into()));
                        }
                        segments.push(PathSeg::Name(
                            s[start..i].to_string(),
                        ));
                    }
                    None => return Err(SelectError::Parse("invalid FROM path".into())),
                }
            }
            b'.' => {
                return Err(SelectError::Parse(
                    "invalid FROM path: must start with S3Object[*]".into(),
                ));
            }
            b'[' if segments.is_empty() && bytes.get(i + 1) != Some(&b'*') => {
                return Err(SelectError::Parse(
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
                        let value = s[start..i].parse::<usize>().map_err(|_| {
                            SelectError::Parse("invalid FROM path".into())
                        })?;
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
                                return Err(SelectError::Parse("invalid FROM path".into()));
                            }
                        }
                        let name = s[start..i].to_string();
                        expect(bytes, i + 1, b']')?;
                        i += 2;
                        segments.push(PathSeg::Name(name));
                    }
                    _ => return Err(SelectError::Parse("invalid FROM path".into())),
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
        let w = take_ident(bytes, &mut j)
            .ok_or_else(|| SelectError::Parse("invalid FROM path".into()))?;
        if clause_keyword(&w) {
            return Err(SelectError::Parse("invalid FROM path".into()));
        }
        alias = Some(w);
    } else if let Some(w) = take_ident(bytes, &mut j).filter(|w| !clause_keyword(w)) {
        alias = Some(w);
    }
    Ok((
        FromClause {
            traversed: !segments.is_empty(),
            segments,
            alias,
        },
        seg_end,
    ))
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

fn expect(bytes: &[u8], i: usize, b: u8) -> Result<(), SelectError> {
    if bytes.get(i) == Some(&b) {
        Ok(())
    } else {
        Err(SelectError::Parse("invalid FROM path".into()))
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
/// `__s3_is_missing(X)` / `__s3_is_not_missing(X)` — sqlparser 0.57 has no
/// `IsMissing` variant, and `IS NULL` substitution would be wrong (a
/// present-but-null field must not satisfy MISSING). Token-based, so
/// strings, quoted identifiers and comments are never entered.
fn rewrite_is_missing(sql: &str) -> String {
    let mut tokenizer = Tokenizer::new(&GenericDialect, sql);
    let Ok(tokens) = tokenizer.tokenize_with_location() else {
        return sql.to_string(); // unterminated literal etc: let the parser report
    };
    let pos = positions(sql);
    let mut found: Vec<(usize, usize, usize, bool)> = Vec::new();
    let mut i = 0;
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
        let operand_start = pos[&token_start(&tokens[operand_token])];
        let is_start = pos[&token_start(&tokens[i])];
        let missing_end = pos[&token_end(&tokens[j])];
        found.push((operand_start, is_start, missing_end, negated));
        i = j + 1;
    }
    if found.is_empty() {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for (operand_start, is_start, missing_end, negated) in found {
        out.push_str(&sql[last..operand_start]);
        out.push_str(if negated { "__s3_is_not_missing(" } else { "__s3_is_missing(" });
        out.push_str(&sql[operand_start..is_start]);
        out.push(')');
        last = missing_end;
    }
    out.push_str(&sql[last..]);
    out
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
fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => is_aggregate_name(&f.name) || function_exprs(f).iter().any(|e| contains_aggregate(e)),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(e) => contains_aggregate(e),
        Expr::Tuple(exprs) => exprs.iter().any(contains_aggregate),
        Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::IsTrue(e) | Expr::IsNotTrue(e)
        | Expr::IsFalse(e) | Expr::IsNotFalse(e) | Expr::IsUnknown(e) | Expr::IsNotUnknown(e) => {
            contains_aggregate(e)
        }
        Expr::IsDistinctFrom(l, r) | Expr::IsNotDistinctFrom(l, r) => {
            contains_aggregate(l) || contains_aggregate(r)
        }
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::InUnnest { expr, array_expr, .. } => {
            contains_aggregate(expr) || contains_aggregate(array_expr)
        }
        Expr::Between { expr, low, high, .. } => {
            contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } | Expr::RLike { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::Collate { expr, .. } => contains_aggregate(expr),
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::AtTimeZone { timestamp, time_zone } => {
            contains_aggregate(timestamp) || contains_aggregate(time_zone)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || conditions.iter().any(
                    |CaseWhen {
                         condition,
                         result,
                     }| { contains_aggregate(condition) || contains_aggregate(result) },
                )
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            contains_aggregate(root)
                || access_chain.iter().any(|a| match a {
                    AccessExpr::Dot(e) => contains_aggregate(e),
                    AccessExpr::Subscript(Subscript::Index { index }) => contains_aggregate(index),
                    AccessExpr::Subscript(Subscript::Slice {
                        lower_bound,
                        upper_bound,
                        stride,
                    }) => {
                        lower_bound.as_ref().is_some_and(contains_aggregate)
                            || upper_bound.as_ref().is_some_and(contains_aggregate)
                            || stride.as_ref().is_some_and(contains_aggregate)
                    }
                })
        }
        _ => false,
    }
}

fn is_aggregate_name(name: &sqlparser::ast::ObjectName) -> bool {
    let [sqlparser::ast::ObjectNamePart::Identifier(part)] = name.0.as_slice() else {
        return false;
    };
    ["count", "sum", "avg", "min", "max"]
        .iter()
        .any(|k| part.value.eq_ignore_ascii_case(k))
}

/// The expression arguments of a function call (aggregates never hide in
/// `Wildcard`/`QualifiedWildcard` args).
fn function_exprs(f: &Function) -> Vec<&Expr> {
    let mut exprs = Vec::new();
    if let FunctionArguments::List(list) = &f.args {
        for arg in &list.args {
            let expr = match arg {
                FunctionArg::Unnamed(e)
                | FunctionArg::Named { arg: e, .. }
                | FunctionArg::ExprNamed { arg: e, .. } => e,
            };
            if let FunctionArgExpr::Expr(e) = expr {
                exprs.push(e);
            }
        }
    }
    exprs
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    use sqlparser::ast::{AccessExpr, Subscript};

    use super::*;

    fn ok(sql: &str) -> QueryPlan {
        parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    #[track_caller]
    fn rej(sql: &str, contains: &str) {
        match parse(sql) {
            Err(e) => {
                let s = e.to_string();
                assert!(s.contains(contains), "{sql}: {s} does not contain {contains:?}");
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
        assert!(!q.from.traversed);
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
        assert!(q.from.traversed);
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
                    FunctionArguments::List(list) => list.args.iter().any(|a| {
                        matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                    }),
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
    fn reject_aggregates_plus_wild() {
        rej("SELECT *, count(*) FROM S3Object s", "aggregates require an explicit select list");
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
    fn traversal_plus_missing_rewrite() {
        let q = ok("SELECT s.x FROM S3Object[*].books[*].price s WHERE s.x IS MISSING");
        assert!(q.from.traversed);
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
        rej("SELECT * FROM S3Object LIMIT -5", "LIMIT must be a positive integer");
        rej("SELECT * FROM S3Object LIMIT 0", "LIMIT must be a positive integer");
    }

    #[test]
    fn expression_too_long() {
        let sql = format!("SELECT s.x FROM S3Object s WHERE s.x = '{}'", "a".repeat(300 * 1024));
        rej(&sql, "expression exceeds 256 KiB");
    }

    #[test]
    fn keyword_named_fields_are_not_keywords() {
        // A path segment spelled like a clause keyword stays a segment.
        let q = ok("SELECT x FROM S3Object[*].limit.name s");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("limit".into()), PathSeg::Name("name".into())]
        );
        // A field named `from` is a field, and its MISSING operand is exact.
        let q = ok("SELECT s.from FROM S3Object s WHERE s.from IS MISSING");
        assert_eq!(q.projections.len(), 1);
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), "__s3_is_missing"),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }
}
