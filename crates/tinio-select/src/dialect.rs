//! Custom sqlparser Dialect: the `IS [NOT] MISSING` hook + the custom FROM
//! factor via the `parse_statement` hook. Every other method delegates to
//! GenericDialect — an incomplete delegation silently changes stock-path
//! behavior (e.g. supports_limit_comma) — and `Dialect::dialect()` reports
//! that same identity, so stock's hardcoded `dialect_of!` guards take the
//! delegated arm instead of silently going false. That one override is what
//! closes `B'…'`/`R'…'`; it is also the reason this dialect is not a faithful
//! wrapper in the two hooks above, which stock never asks about by identity.

use std::{
    any::TypeId,
    cell::{Cell, RefCell},
};

use delegate::delegate;
use sqlparser::{
    ast::{
        Distinct, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
        FunctionArguments, GroupByExpr, Ident, LimitClause, ObjectName, ObjectNamePart, Offset,
        Query, Select, SelectFlavor, SetExpr, SetOperator, Statement, TableAlias, TableFactor,
        TableWithJoins, UnaryOperator, helpers::attached_token::AttachedToken,
    },
    dialect::{Dialect, GenericDialect},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

use crate::{
    engine::is_sentinel_call,
    path::{INVALID_FROM_PATH, PathSeg, SpanCursor},
    sql::{CLAUSE_KEYWORDS, EXPECTED_S3OBJECT_MSG, JOIN_KEYWORDS, S3_OBJECT, SentinelNames},
};

// Debug is required by the Dialect trait; String (not &str) because
// Dialect: Any imposes 'static.
#[derive(Debug)]
pub struct S3SelectDialect {
    inner: GenericDialect,
    missing: SentinelNames,
    /// Path segments captured by the custom FROM factor; parse() takes them
    /// before building the QueryPlan. The alias is not captured — it lives
    /// on the AST factor (`validate_from` reads it), one source only.
    captured: RefCell<Option<Vec<PathSeg>>>,
    /// The statement text under parse: the custom factor slices the source
    /// here for the pest prefix parse (spans carry line/col only).
    sql: String,
    /// Custom-FROM-factor candidate, computed once here (one linear
    /// tokenize, see `has_custom_path_candidate`).
    candidate: bool,
    /// Set when the skeleton rejects a JOIN-family word behind the custom
    /// factor, so `parse()` can map it back to `Error::Unsupported` (the old
    /// channel). One fact: the marker is only ever the JOIN rejection.
    join_rejected: Cell<bool>,
}

impl S3SelectDialect {
    pub fn new(missing: SentinelNames, sql: &str) -> Self {
        Self {
            inner: GenericDialect,
            missing,
            captured: RefCell::new(None),
            sql: sql.to_string(),
            candidate: has_custom_path_candidate(sql, &GenericDialect),
            join_rejected: Cell::new(false),
        }
    }

    /// Take the path segments captured this parse (custom-path statements
    /// only; the alias is read from the AST factor).
    pub fn take_segments(&self) -> Option<Vec<PathSeg>> {
        self.captured.borrow_mut().take()
    }

    /// Take the JOIN-family rejection marker set during this parse, if any.
    pub fn take_join_rejected(&self) -> bool {
        self.join_rejected.replace(false)
    }

    /// Custom SELECT: mirrors the stock clause order (parse_select), with
    /// the FROM factor parsed by parse_custom_factor.
    fn parse_custom_select(&self, parser: &mut Parser) -> Result<Statement, ParserError> {
        parser.next_token(); // SELECT (the parse_statement gate verified it)
        let distinct = if parser.parse_keyword(Keyword::DISTINCT) {
            Some(Distinct::Distinct)
        } else {
            None
        };
        let projection = parser.parse_projection()?;
        if !parser.parse_keyword(Keyword::FROM) {
            return Err(parser_error(EXPECTED_S3OBJECT_MSG));
        }
        let (segs, alias) = self.parse_custom_factor(parser)?;
        let selection = if parser.parse_keyword(Keyword::WHERE) {
            Some(parser.parse_expr()?)
        } else {
            None
        };
        let group_by = parser.parse_optional_group_by()?;
        let having = if parser.parse_keyword(Keyword::HAVING) {
            Some(parser.parse_expr()?)
        } else {
            None
        };
        let order_by = parser.parse_optional_order_by()?;
        let limit_clause = self.parse_optional_limit_clause(parser)?;
        // The old pipeline surfaced a JOIN behind the custom factor as
        // Unsupported("JOIN") (splice → stock parse → validator); the
        // skeleton parses no joins, so it rejects at the clause with the
        // same channel and message: the marker makes parse() re-map this
        // error to Error::Unsupported("JOIN") (restored 2026-09-10 —
        // structured, never sniffed from the text; the parser_error text
        // is what parse() discards when the marker is set).
        if is_join_keyword(&parser.peek_token()) {
            self.join_rejected.set(true);
            // Fallback text for a direct-dialect caller: parse() prefers the
            // marker, not this string.
            return Err(parser_error("unsupported: JOIN"));
        }

        let select = Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hints: Vec::new(),
            distinct,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(S3_OBJECT))]),
                    alias,
                    args: None,
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            connect_by: Vec::new(),
            group_by: group_by.unwrap_or(GroupByExpr::Expressions(Vec::new(), Vec::new())),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        });
        let body = match parser.parse_set_operator(&parser.peek_token().token) {
            Some(op) => {
                // Precedence numbers mirror stock's values for the set
                // operators; they only need to let the right operand parse —
                // the validator rejects every SetOperation anyway.
                let precedence = if op == SetOperator::Intersect { 20 } else { 10 };
                parser.next_token(); // set-op word
                let set_quantifier = parser.parse_set_quantifier(&Some(op));
                let right = parser.parse_query_body(precedence)?;
                SetExpr::SetOperation {
                    left: Box::new(SetExpr::Select(select)),
                    op,
                    set_quantifier,
                    right,
                }
            }
            None => SetExpr::Select(select),
        };
        *self.captured.borrow_mut() = Some(segs);
        Ok(Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(body),
            order_by,
            limit_clause,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: Vec::new(),
        })))
    }

    /// `S3Object` + path segments (pest prefix parse) → (segments, alias).
    fn parse_custom_factor(
        &self,
        parser: &mut Parser,
    ) -> Result<(Vec<PathSeg>, Option<TableAlias>), ParserError> {
        let tok = parser.next_token(); // S3Object word
        let Token::Word(w) = &tok.token else {
            return Err(parser_error(EXPECTED_S3OBJECT_MSG));
        };
        if w.quote_style.is_some() || !w.value.eq_ignore_ascii_case(S3_OBJECT) {
            return Err(parser_error(EXPECTED_S3OBJECT_MSG));
        }
        // Slice from the S3Object token's START byte (the pest `path` rule
        // re-matches the `^S3Object` root itself — slicing from the token's
        // end would make every factor fail).
        // One monotonic cursor for the whole factor: the span lookups below
        // are consumed in strictly increasing document order (S3Object start
        // → its end → each following token's start → its end), so each seek
        // walks only the gap since the previous one — O(statement) per
        // factor, not O(tokens × statement) (2026-09-10).
        let mut cursor = SpanCursor::new(&self.sql);
        let start_byte = cursor
            .seek(tok.span.start.line, tok.span.start.column)
            .ok_or_else(|| parser_error(INVALID_FROM_PATH))?;
        let slice = &self.sql[start_byte..];
        let (segs, consumed) = PathSeg::parse(slice).map_err(|e| parser_error(&e))?;
        let path_end = start_byte + consumed;

        // Consume tokens while token start < path_end. Boundary-alignment
        // check (a): the last consumed token's END byte must equal path_end.
        // `last_end` is SEEDED with the S3Object token's own end so the
        // zero-segment case (`FROM S3Object s` via a false-positive trigger,
        // where the walk consumes nothing) passes instead of erroring.
        let mut last_end = cursor
            .seek(tok.span.end.line, tok.span.end.column)
            .ok_or_else(|| parser_error(INVALID_FROM_PATH))?;
        loop {
            let t = parser.peek_token();
            if matches!(t.token, Token::EOF) {
                break;
            }
            let start = cursor
                .seek(t.span.start.line, t.span.start.column)
                .ok_or_else(|| parser_error(INVALID_FROM_PATH))?;
            if start >= path_end {
                break;
            }
            parser.next_token();
            last_end = cursor
                .seek(t.span.end.line, t.span.end.column)
                .ok_or_else(|| parser_error(INVALID_FROM_PATH))?;
        }
        if last_end != path_end {
            // (a) pest stopped inside a token: the Word class is wider than
            // pest `name` (non-ASCII, @, #) — reject, never truncate-accept.
            return Err(parser_error(INVALID_FROM_PATH));
        }
        // Refused continuation (b): a path directly followed by Period /
        // LBracket is a segment the grammar refused (`S3Object.books`,
        // `[0x]`, `['it''s']`, `S3Object[0]`) — the pest prefix parse never
        // fails on its own (the segment group is optional).
        let next = parser.peek_token();
        if matches!(next.token, Token::Period | Token::LBracket) {
            return Err(parser_error(INVALID_FROM_PATH));
        }
        // Alias: hand-rolled (stock parse_optional_alias's after_as arm
        // accepts ANY keyword — parser/mod.rs:12953-12960 — which would
        // loosen the `AS where` rejection; keep the deleted byte-scanner's
        // ident+clause-keyword rules).
        let alias = self.parse_custom_alias(parser)?;
        Ok((segs, alias))
    }

    /// `AS? ident`; an unquoted clause keyword is never an alias (bare: the
    /// clause starts; after AS: rejected, as the old pipeline did); any
    /// other keyword word is not an alias either (the old pipeline let the
    /// stock tail reject it — the skeleton must reject it itself).
    fn parse_custom_alias(&self, parser: &mut Parser) -> Result<Option<TableAlias>, ParserError> {
        let after_as = parser.parse_keyword(Keyword::AS);
        let t = parser.peek_token();
        let is_clause = |t: &TokenWithSpan| CLAUSE_KEYWORDS.iter().any(|k| unquoted_word_eq(t, k));
        let ident = match &t.token {
            Token::Word(_) if is_clause(&t) => {
                if after_as {
                    // `AS where` — old rejection kept.
                    return Err(parser_error(INVALID_FROM_PATH));
                }
                None // WHERE/GROUP/... starts the next clause
            }
            Token::Word(w) if w.quote_style.is_some() || w.keyword == Keyword::NoKeyword => {
                let value = w.value.clone();
                let quote = w.quote_style;
                parser.next_token();
                Some(match quote {
                    Some(q) => Ident::with_quote(q, value),
                    None => Ident::new(value),
                })
            }
            // Anything else (`select`, `and`, a non-word token...): not an
            // alias. Bare → leave it for the clause dispatch / trailing-token
            // error; after AS → reject outright. Both were rejections in the
            // old pipeline (via the stock tail).
            _ if after_as => return Err(parser_error(INVALID_FROM_PATH)),
            _ => None,
        };
        Ok(ident.map(|name| TableAlias {
            explicit: after_as,
            name,
            columns: Vec::new(),
            at: None,
        }))
    }

    /// `LIMIT <n> [BY <expr>[, <expr>]*] [OFFSET <expr> [ROW|ROWS]]` for the
    /// custom path. `parse_optional_limit_clause` is a private stock API, so
    /// this hand-rolled form mirrors its `LimitOffset` shape (the stock
    /// public `parse_comma_separated`/`parse_offset` do the sub-parsing).
    /// `BY` and `OFFSET` stay in the clause and the validator's
    /// `Unsupported("LIMIT BY")`/`Unsupported("OFFSET")` arms reject them —
    /// one acceptance gate, no duplication. The MySQL comma form
    /// (`LIMIT 5,2`) is not parsed here: the comma remains, fails in the
    /// statement tail, and the request is rejected closed (the stock path
    /// keeps its own handling).
    fn parse_optional_limit_clause(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<LimitClause>, ParserError> {
        if !parser.parse_keyword(Keyword::LIMIT) {
            return Ok(None);
        }
        let limit = Some(parser.parse_expr()?);
        let limit_by: Vec<Expr> = if parser.parse_keyword(Keyword::BY) {
            parser.parse_comma_separated(Parser::parse_expr)?
        } else {
            Vec::new()
        };
        let offset: Option<Offset> = if parser.parse_keyword(Keyword::OFFSET) {
            Some(parser.parse_offset()?)
        } else {
            None
        };
        Ok(Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }))
    }
}

/// Does the input carry a custom-FROM-factor candidate: an unquoted
/// `S3Object` word followed (whitespace aside) by `[` or `.`? One linear
/// tokenize at construction (the tokenizer owns quote/comment state).
/// Fail-open on a tokenizer error — `parse_sql` reports the real error. Whole-input,
/// not per-statement: multi-statement inputs are rejected by the
/// single-statement check anyway, and a false positive on a pathless SELECT
/// yields an equivalent AST (pinned by
/// `false_positive_projection_trigger_keeps_stock_behavior`).
fn has_custom_path_candidate(sql: &str, dialect: &GenericDialect) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize_with_location() else {
        return true;
    };
    let mut i = 0;
    while i < tokens.len() {
        if let Token::Word(w) = &tokens[i].token
            && w.quote_style.is_none()
            && w.value.eq_ignore_ascii_case(S3_OBJECT)
        {
            let mut j = i + 1;
            while matches!(tokens.get(j).map(|t| &t.token), Some(Token::Whitespace(_))) {
                j += 1;
            }
            if matches!(
                tokens.get(j).map(|t| &t.token),
                Some(Token::LBracket | Token::Period)
            ) {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Unquoted, case-insensitive match of a word token against `word`.
fn unquoted_word_eq(t: &TokenWithSpan, word: &str) -> bool {
    match &t.token {
        Token::Word(w) if w.quote_style.is_none() => w.value.eq_ignore_ascii_case(word),
        _ => false,
    }
}

/// JOIN-family word (unquoted, case-insensitive): `JOIN` plus the keywords
/// that can only appear as part of a join (`sql::JOIN_KEYWORDS`). A join
/// behind the custom factor is never valid — the skeleton rejects it up
/// front with the old pipeline's channel and message (Unsupported("JOIN"))
/// instead of leaving a raw "Expected: end of statement" tail.
fn is_join_keyword(t: &TokenWithSpan) -> bool {
    JOIN_KEYWORDS.iter().any(|k| unquoted_word_eq(t, k))
}

/// Text-only ParserError constructor (the `ParserError(String)` variant is
/// public in 0.62).
fn parser_error(msg: &str) -> ParserError {
    ParserError::ParserError(msg.into())
}

/// Sentinel call: single-part, unquoted name, one argument (the shape the
/// engine's sentinel_operand expects).
fn sentinel_function(name: &str, operand: &Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            clauses: Vec::new(),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(operand.clone()))],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

impl Dialect for S3SelectDialect {
    // -----------------------------------------------------------------
    // GenericDialect delegation: one `delegate!` list mirroring generic.rs's
    // 71 overrides (the user directive 2026-09-10 supersedes the earlier
    // hand-written forwarders). The macro emits only what is listed, so a
    // method missing from the list silently falls back to the trait default
    // — the list is re-audited against `dialect/generic.rs` on any sqlparser
    // bump and pinned by the delegation-drift corpus
    // (`delegation_drift_surface`) plus the existing behavior tests.
    // -----------------------------------------------------------------
    delegate! {
        to self.inner {
            fn is_delimited_identifier_start(&self, ch: char) -> bool;
            fn is_identifier_start(&self, ch: char) -> bool;
            fn is_identifier_part(&self, ch: char) -> bool;
            fn supports_unicode_string_literal(&self) -> bool;
            fn supports_partition_by_after_order_by(&self) -> bool;
            fn supports_array_join_syntax(&self) -> bool;
            fn supports_group_by_expr(&self) -> bool;
            fn supports_group_by_with_modifier(&self) -> bool;
            fn supports_left_associative_joins_without_parens(&self) -> bool;
            fn supports_connect_by(&self) -> bool;
            fn supports_match_recognize(&self) -> bool;
            fn supports_pipe_operator(&self) -> bool;
            fn supports_start_transaction_modifier(&self) -> bool;
            fn supports_window_function_null_treatment_arg(&self) -> bool;
            fn supports_dictionary_syntax(&self) -> bool;
            fn supports_window_clause_named_window_reference(&self) -> bool;
            fn supports_parenthesized_set_variables(&self) -> bool;
            fn supports_select_wildcard_except(&self) -> bool;
            fn support_map_literal_syntax(&self) -> bool;
            fn allow_extract_custom(&self) -> bool;
            fn allow_extract_single_quotes(&self) -> bool;
            fn supports_extract_comma_syntax(&self) -> bool;
            fn supports_create_view_comment_syntax(&self) -> bool;
            fn supports_parens_around_table_factor(&self) -> bool;
            fn supports_values_as_table_factor(&self) -> bool;
            fn supports_create_index_with_clause(&self) -> bool;
            fn supports_explain_with_utility_options(&self) -> bool;
            fn supports_limit_comma(&self) -> bool;
            fn supports_update_order_by(&self) -> bool;
            fn supports_from_first_select(&self) -> bool;
            fn supports_projection_trailing_commas(&self) -> bool;
            fn supports_asc_desc_in_column_definition(&self) -> bool;
            fn supports_try_convert(&self) -> bool;
            fn supports_bitwise_shift_operators(&self) -> bool;
            fn supports_comment_on(&self) -> bool;
            fn supports_load_extension(&self) -> bool;
            fn supports_named_fn_args_with_assignment_operator(&self) -> bool;
            fn supports_struct_literal(&self) -> bool;
            fn supports_empty_projections(&self) -> bool;
            fn supports_nested_comments(&self) -> bool;
            fn supports_multiline_comment_hints(&self) -> bool;
            fn supports_user_host_grantee(&self) -> bool;
            fn supports_string_escape_constant(&self) -> bool;
            fn supports_array_typedef_with_brackets(&self) -> bool;
            fn supports_match_against(&self) -> bool;
            fn supports_set_names(&self) -> bool;
            fn supports_comma_separated_set_assignments(&self) -> bool;
            fn supports_filter_during_aggregation(&self) -> bool;
            fn supports_select_wildcard_exclude(&self) -> bool;
            fn supports_data_type_signed_suffix(&self) -> bool;
            fn supports_interval_options(&self) -> bool;
            fn supports_quote_delimited_string(&self) -> bool;
            fn supports_select_wildcard_replace(&self) -> bool;
            fn supports_select_wildcard_ilike(&self) -> bool;
            fn supports_select_wildcard_rename(&self) -> bool;
            fn supports_optimize_table(&self) -> bool;
            fn supports_install(&self) -> bool;
            fn supports_detach(&self) -> bool;
            fn supports_prewhere(&self) -> bool;
            fn supports_with_fill(&self) -> bool;
            fn supports_limit_by(&self) -> bool;
            fn supports_interpolate(&self) -> bool;
            fn supports_settings(&self) -> bool;
            fn supports_select_format(&self) -> bool;
            fn supports_comment_optimizer_hint(&self) -> bool;
            fn supports_constraint_keyword_without_name(&self) -> bool;
            fn supports_key_column_option(&self) -> bool;
            fn supports_comma_separated_trim(&self) -> bool;
            fn supports_cte_without_as(&self) -> bool;
            fn supports_select_item_multi_column_alias(&self) -> bool;
            fn supports_xml_expressions(&self) -> bool;
        }
    }

    // -----------------------------------------------------------------
    // Dialect identity: stock's `dialect_of!`/`dialect_is!` guards
    // -----------------------------------------------------------------
    /// Report `GenericDialect`'s `TypeId`, so every `dialect_of!` /
    /// `dialect_is!` guard in stock behaves as it does for the dialect this
    /// one delegates to. Both macros expand to `x.dialect.is::<T>()`, i.e.
    /// `TypeId::of::<T>() == self.dialect()`, and the trait default returns
    /// *this* type's `TypeId` — under which all 74 `GenericDialect`-naming
    /// guards read false and silently take the wrong arm. The byte/raw
    /// string prefixes (`tokenizer.rs:1085`/`1125`) are the sharp edge: with
    /// them false, `B'1'` tokenizes as the identifier `B` aliased `'1'`, and
    /// `SELECT B'1' FROM S3Object s` parsed to MISSING instead of being
    /// refused. See `Dialect::dialect`'s own doc ("overridden by dialects
    /// that behave like other dialects") and its `parse_with_wrapped_dialect`
    /// test. The `delegate!` list above is unaffected: it forwards methods,
    /// not identity.
    fn dialect(&self) -> TypeId {
        // Forwarded, not written down: "the identity of the dialect I delegate
        // to". Today that is `GenericDialect`'s own default, but if it ever
        // overrides `dialect()` the delegation follows it here too.
        self.inner.dialect()
    }

    // -----------------------------------------------------------------
    // Custom statement hook
    // -----------------------------------------------------------------
    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // Two gates: the statement starts with an unquoted SELECT keyword,
        // and the input carries an `S3Object[` / `S3Object.` candidate
        // (computed once in `new` — see `has_custom_path_candidate`).
        // Nothing is consumed before deciding — a decline re-dispatches
        // stock from the same token position (this hook runs before the
        // leading token is consumed).
        let first = parser.peek_token();
        let is_select = matches!(
            &first.token,
            Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::SELECT
        );
        if !is_select || !self.candidate {
            return None; // WITH/CREATE/INSERT/UNION-led, or no path candidate → stock
        }
        Some(self.parse_custom_select(parser))
    }

    // -----------------------------------------------------------------
    // IS [NOT] MISSING
    // -----------------------------------------------------------------
    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        // The current token must be IS (not yet consumed).
        if !parser.peek_keyword(Keyword::IS) {
            return None;
        }
        // Look ahead (peek consumes nothing): optional NOT — compared by
        // KEYWORD, never by Token::make_keyword("NOT") equality (Word::value
        // keeps the user's casing, so equality would miss lowercase `not`
        // that the old byte scan accepted) — then a `missing` word.
        let mut n = 1;
        let negated = matches!(
            &parser.peek_nth_token(n).token,
            Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::NOT
        );
        if negated {
            n += 1;
        }
        if !is_missing_word(&parser.peek_nth_token(n)) {
            return None; // IS NOT NULL / IS DISTINCT FROM etc. → stock
        }
        // Confirmed: consume IS [NOT] MISSING (next_token skips whitespace —
        // advance_token's loop, parser/mod.rs:4528). The keyword peeks above
        // already matched, so the consumptions cannot fail.
        let _ = parser.parse_keyword(Keyword::IS);
        if negated {
            let _ = parser.parse_keyword(Keyword::NOT);
        }
        parser.next_token(); // the `missing` word

        // Chained rejection: in `X IS [NOT] MISSING IS MISSING` the operand
        // is already a sentinel — a bare Function, or our own UnaryOp{Not}
        // wrapper (the NOT is this hook's production for IS NOT MISSING,
        // not a user Nested). Both shapes are rejected.
        let sentinel_name = self.missing.is_missing();
        if is_sentinel_expr(expr, &sentinel_name) {
            return Some(Err(ParserError::ParserError(
                "invalid IS MISSING expression".into(),
            )));
        }

        // One name: IS NOT MISSING = UnaryOp{Not} around the sentinel call.
        let call = sentinel_function(&sentinel_name, expr);
        Some(Ok(if negated {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(call),
            }
        } else {
            call
        }))
    }
}

/// Unquoted, case-insensitive `missing` word (a NoKeyword token).
fn is_missing_word(t: &TokenWithSpan) -> bool {
    unquoted_word_eq(t, "missing")
}

/// Is the expression already a sentinel call: a bare `Function(sentinel)`,
/// this hook's own `UnaryOp{Not}` wrapper (the shape of a chained
/// IS NOT MISSING IS MISSING), or its `Nested` wrapper (the shape of a
/// chained `(X IS MISSING) IS MISSING`). The Function arm is the engine's
/// own `is_sentinel_call` (one definition of sentinel identity); the
/// UnaryOp{Not} arm recurses into its operand; depth bounded by the
/// parser's recursion guard. A `Nested` over a non-sentinel (`(s.a) IS
/// MISSING`) stays a plain operand: the recursion only follows the hook's
/// own productions.
fn is_sentinel_expr(expr: &Expr, sentinel: &str) -> bool {
    match expr {
        Expr::Function(f) => is_sentinel_call(&f.name, sentinel),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => is_sentinel_expr(expr, sentinel),
        Expr::Nested(e) => is_sentinel_expr(e, sentinel),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::Dialect;

    use super::*;

    /// The `dialect()` override is the whole fix for the byte/raw-string
    /// prefixes, and it is a one-value contract: every `dialect_of!` /
    /// `dialect_is!` guard in stock reduces to `TypeId::of::<T>() ==
    /// self.dialect()`. Pinned here because a `delegate!` re-audit (or a
    /// future `dialect()` of our own) could silently take it back out —
    /// the observable, `SELECT B'1' FROM S3Object s` answering 400 instead
    /// of a MISSING column, is pinned separately in `engine.rs`.
    #[test]
    fn identity_is_generic_so_stock_guards_match_the_delegate() {
        let dialect = S3SelectDialect::new(SentinelNames::mint(), "SELECT 1");
        assert_eq!(dialect.dialect(), TypeId::of::<GenericDialect>());
        // The counter-assertion is against the trait *default* (`self.type_id()`,
        // the value every stock guard reads as false), not against
        // `GenericDialect.dialect()` — that method is the default too, so the
        // comparison would re-check the line above through the same impl and
        // could never fail. Delete the override and this goes red.
        assert_ne!(dialect.dialect(), TypeId::of::<S3SelectDialect>());
    }
}
