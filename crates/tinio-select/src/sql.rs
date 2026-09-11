//! SQL parse + validate: sqlparser through the custom dialect (`parse_statement`
//! for the custom FROM factor + the `IS [NOT] MISSING` hook) + a
//! restrict-grammar validator producing the engine's `QueryPlan`.

use std::ops::ControlFlow;

use sqlparser::{
    ast::{
        DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
        LimitClause, ObjectName, ObjectNamePart, Query, Select, SelectFlavor, SelectItem, SetExpr,
        Statement, TableFactor, Value, Visit, Visitor,
    },
    parser::Parser,
};
use uuid::Uuid;

// Re-export: `FromClause.segments` keeps the public path `sql::PathSeg`.
pub use crate::path::PathSeg;
use crate::{dialect::S3SelectDialect, engine, error::Error};

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

/// Per-request MISSING sentinel. Only the uuid is stored; the full function
/// name is `__s3_is_missing_<uuid>` (the prefix is a debug/tracing marker —
/// not a reserved-name attack surface; the unguessable part is the uuid).
/// `IS NOT MISSING` reuses the same name: `UnaryOp{Not}` wraps the call.
/// An earlier design held two fixed names (`__s3_is_missing` /
/// `__s3_is_not_missing`); now one per-request uuid, so user text can
/// never collide with the sentinel (review R1).
#[derive(Debug, Clone, PartialEq)]
pub struct SentinelNames {
    /// Private: minted only via `mint()` (review 2026-09-10).
    uuid: Uuid,
}

impl SentinelNames {
    /// Mint the request's sentinel names.
    pub fn mint() -> Self {
        Self {
            uuid: Uuid::new_v4(),
        }
    }

    /// This request's full sentinel function name — the crate's single
    /// construction point.
    pub fn is_missing(&self) -> String {
        format!("__s3_is_missing_{}", self.uuid.simple())
    }
}

/// Validated query plan consumed by the engine (`eval`, `json::Reader`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub from: FromClause,
    pub projections: Vec<Projection>,
    pub where_expr: Option<Expr>,
    pub limit: Option<usize>,
    pub aggregates: bool,
    pub missing: SentinelNames,
}

/// AWS documented expression limit (review 2026-09-05 #5).
const MAX_EXPRESSION: usize = 256 * 1024;

/// The FROM factor name, spelled once (the dialect's factor match and AST
/// construction and the validator all read it here).
pub(crate) const S3_OBJECT: &str = "S3Object";

/// Not-the-`S3Object` rejection: the dialect's factor match and the
/// validator's quoted / mismatched-name arms share this literal.
pub(crate) const EXPECTED_S3OBJECT_MSG: &str = "invalid FROM: expected S3Object";

/// The not-exactly-one-factor rejection: zero or several factors, a
/// non-table factor, or a non-identifier object-name part.
pub(crate) const ONE_S3OBJECT_MSG: &str = "FROM must reference exactly one S3Object";

/// The JOIN construct kind, spelled once: the validator's joins arm and the
/// skeleton's marker → `Error::Unsupported` mapping both read it.
pub(crate) const JOIN_KIND: &str = "JOIN";

/// The JOIN-family words: `JOIN` plus the keywords that can only appear as
/// part of a join. Single authority for the dialect's `is_join_keyword`
/// rejection; a test asserts every entry is also in `CLAUSE_KEYWORDS`.
pub(crate) const JOIN_KEYWORDS: [&str; 9] = [
    "join", "left", "right", "inner", "cross", "full", "natural", "on", "using",
];

/// Keywords that can never serve as the FROM alias (they start the next
/// clause, or they are join/table operators). Shared with the dialect's
/// custom-alias parser.
pub(crate) const CLAUSE_KEYWORDS: &[&str] = &[
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

/// Parse + validate one S3 Select expression.
pub fn parse(sql: &str) -> Result<QueryPlan, Error> {
    if sql.len() > MAX_EXPRESSION {
        return Err(Error::Parse("expression exceeds 256 KiB".into()));
    }
    let missing = SentinelNames::mint();
    // A SELECT carrying an `S3Object[` / `S3Object.` candidate is parsed by
    // the dialect's parse_statement skeleton (the custom FROM factor);
    // everything else goes down the stock parse + validate_from path.
    // `IS MISSING` stays as written — the dialect's parse_infix hook takes it.
    let dialect = S3SelectDialect::new(missing.clone(), sql);
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(e) => {
            // The skeleton marks its JOIN-family rejection so it keeps the
            // old validator's Unsupported channel instead of a Parse
            // wrapper; every other parser error is an honest syntax error
            // (LIMIT BY / OFFSET are not marked — the validator's own arms
            // reject them after a successful parse).
            return Err(if dialect.take_join_rejected() {
                Error::Unsupported(JOIN_KIND.into())
            } else {
                Error::Parse(format!("syntax error: {e}"))
            });
        }
    };
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
    reject_unread_clauses(query, select)?;
    reject_empty_projection(select)?;
    reject_wildcard_options(select)?;
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
    // One FromClause construction for both paths: the segments come from
    // the dialect's captured slot (empty for a non-custom statement), the
    // alias from the single validated AST factor.
    let segments = dialect.take_segments().unwrap_or_default();
    let from = validate_from(select, segments)?;
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
    reject_unsupported_expressions(select, &missing)?;
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
        missing,
    })
}

/// LIMIT must be a positive integer literal.
fn positive_limit(limit: &Expr) -> Result<Option<usize>, Error> {
    match limit {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => match n.parse::<usize>() {
                Ok(n) if n > 0 => Ok(Some(n)),
                _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
            },
            _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
        },
        _ => Err(Error::Parse("LIMIT must be a positive integer".into())),
    }
}

/// One declined clause or FROM-factor field, reported by name — the refusal
/// constructor the field-allowlist checks (`validate_from`,
/// `reject_unread_clauses`) share.
fn unsupported<T>(name: &str) -> Result<T, Error> {
    Err(Error::Unsupported(name.into()))
}

/// The FROM clause must be exactly the single `S3Object` factor; builds the
/// `FromClause` from the caller's segments and the factor's own alias — the
/// single source for both paths (custom-path statements carry the dialect's
/// captured segments, non-custom ones an empty list; the AST factor is the
/// same plain `S3Object` + alias shape either way).
fn validate_from(select: &Select, segments: Vec<PathSeg>) -> Result<FromClause, Error> {
    let twj = match select.from.as_slice() {
        [t] => t,
        _ => return Err(Error::Parse(ONE_S3OBJECT_MSG.into())),
    };
    if !twj.joins.is_empty() {
        return Err(Error::Unsupported(JOIN_KIND.into()));
    }
    // Every field of the FROM factor the engine does not read must be at its
    // default, for the same reason `reject_unread_clauses` exists: `FromClause`
    // carries the path segments and the alias and nothing else, so any other
    // field is dropped on the floor. The `..` this replaces did exactly that —
    // `PARTITION (p)`, `S3Object(p)`, `WITH (a)` and `TABLESAMPLE (10)` all
    // parsed and then quietly meant something else.
    //
    // The alias's own column list (`S3Object s (a, b)`) was the last hole of
    // this class — `FromClause.alias` is the name string and nothing else, so
    // the columns parsed and then silently meant nothing. It has its own arm
    // below (`TableAlias::columns`, a `TableAlias` field rather than a
    // `TableFactor` one). `TableAlias`'s other fields need no arm: `name` is
    // read, `explicit` carries no semantic, and `at` — PartiQL's `AT index` —
    // is set only behind `supports_partiql`, which this dialect does not
    // delegate, so it is dead here as `version`/`json_path` are.
    //
    // Three of the eight arms below are dead under this dialect for the same
    // reason — `version` (needs `supports_table_versioning`), `json_path`
    // (needs `supports_partiql`), `index_hints` (needs `supports_table_hints`)
    // — none is in `dialect.rs`'s `delegate!` list, so all three stay `false`.
    // Kept as future-proofing so a dialect change cannot open a silent hole —
    // the standing `reject_unread_clauses` gives its own dead arms.
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &twj.relation
    else {
        return Err(Error::Parse(ONE_S3OBJECT_MSG.into()));
    };
    if !partitions.is_empty() {
        return unsupported("PARTITION");
    }
    if args.is_some() {
        return unsupported("table-function args");
    }
    if !with_hints.is_empty() {
        return unsupported("WITH");
    }
    if version.is_some() {
        return unsupported("VERSION");
    }
    if *with_ordinality {
        return unsupported("WITH ORDINALITY");
    }
    if json_path.is_some() {
        return unsupported("JSON path");
    }
    if sample.is_some() {
        return unsupported("TABLESAMPLE");
    }
    if !index_hints.is_empty() {
        return unsupported("index hints");
    }
    // The alias's column list: `FromClause` keeps the alias *name*, so
    // `S3Object s (a, b)` named two columns that went nowhere — refusing the
    // factor is the honest answer, exactly as for the eight fields above.
    if let Some(alias) = alias
        && !alias.columns.is_empty()
    {
        return unsupported("alias column list");
    }
    let [ObjectNamePart::Identifier(name_ident)] = name.0.as_slice() else {
        // sqlparser 0.62 added `ObjectNamePart::Function` (a call in
        // object-name position) — never a valid FROM factor here.
        return Err(Error::Parse(ONE_S3OBJECT_MSG.into()));
    };
    if name_ident.quote_style.is_some() {
        // Quoted "S3Object": the old scanner rejected it by accident, the
        // new grammar rejects it explicitly.
        return Err(Error::Parse(EXPECTED_S3OBJECT_MSG.into()));
    }
    if !name_ident.value.eq_ignore_ascii_case(S3_OBJECT) {
        return Err(Error::Parse(EXPECTED_S3OBJECT_MSG.into()));
    }
    Ok(FromClause {
        segments,
        alias: alias.as_ref().map(|a| a.name.value.clone()),
    })
}

/// Every `Select`/`Query` field the engine does not read must be at its
/// default. `QueryPlan` carries six fields, so a clause with no field cannot
/// reach the engine — accepting one would silently drop it. Every field
/// listed here is non-AWS syntax, so over-rejecting is impossible.
///
/// The two destructures below carry no `..`, and that is the guard: a
/// sqlparser bump that adds a field to `Query` or `Select` stops compiling
/// here until someone classifies it — read by `parse` (bound with `_`, each
/// already validated or consumed before this call), refused below by name, or
/// exempt with its reason. The `..` this replaces accepted and dropped a new
/// field without a word; `validate_from` binding all ten factor fields is the
/// same guard on the FROM side, and the reason `PARTITION` cannot come back.
///
/// `flavor` is not an `Option` and cannot be tested for emptiness: it is a
/// plain enum. Asserting `Standard` is what rejects the `FROM`-first forms
/// (`FromFirst`, `FromFirstNoSelect`), both reachable under GenericDialect.
///
/// `optimizer_hints` is the one deliberate exception — a hint is advisory by
/// definition, so dropping it is the correct semantic — and `select_token` is
/// pure token bookkeeping. Both are named here so the per-bump audit does not
/// rediscover them.
fn reject_unread_clauses(query: &Query, select: &Select) -> Result<(), Error> {
    // Read by `parse` before this call — `body` is the `SetExpr` arm, `with`,
    // `order_by`, `distinct`, `group_by` and `having` have their own refusals
    // above, `limit_clause` is the LIMIT block, `from` is `validate_from`,
    // `projection` is the projection loop and `selection` is both the WHERE
    // checks and `QueryPlan.where_expr`. Bound with `_` rather than re-tested:
    // a second verdict on a field `parse` has already decided is how the two
    // drift apart.
    let Query {
        with: _,
        body: _,
        order_by: _,
        limit_clause: _,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;
    // `optimizer_hints` (advisory by definition) and `select_token` (pure
    // token bookkeeping) are the two exempt fields: bound for the audit,
    // never refused.
    let Select {
        select_token: _,
        optimizer_hints: _,
        distinct: _,
        select_modifiers,
        top,
        top_before_distinct,
        projection: _,
        exclude,
        into,
        from: _,
        lateral_views,
        prewhere,
        selection: _,
        connect_by,
        group_by: _,
        cluster_by,
        distribute_by,
        sort_by,
        having: _,
        named_window,
        qualify,
        window_before_qualify,
        value_table_mode,
        flavor,
    } = select;
    // Everything bound without a `_` above is refused here, by name, in this
    // order.
    if top.is_some() || *top_before_distinct {
        return unsupported("TOP");
    }
    if into.is_some() {
        return unsupported("INTO");
    }
    if qualify.is_some() {
        return unsupported("QUALIFY");
    }
    if !named_window.is_empty() || *window_before_qualify {
        return unsupported("WINDOW");
    }
    if !lateral_views.is_empty() {
        return unsupported("LATERAL VIEW");
    }
    if prewhere.is_some() {
        return unsupported("PREWHERE");
    }
    if !connect_by.is_empty() {
        return unsupported("CONNECT BY");
    }
    if !cluster_by.is_empty() {
        return unsupported("CLUSTER BY");
    }
    if !distribute_by.is_empty() {
        return unsupported("DISTRIBUTE BY");
    }
    if !sort_by.is_empty() {
        return unsupported("SORT BY");
    }
    if fetch.is_some() {
        return unsupported("FETCH");
    }
    // A separate field and a different construct: MSSQL's `FOR XML`/`FOR JSON`/
    // `FOR BROWSE` share the `for_clause` slot, so borrowing the lock clause's
    // label reported `FOR UPDATE` for all of them (`parse_for_clause` is
    // dialect-ungated, so they really do reach here). Homogeneous checks — all
    // field presence — so they read as a table and each name sits beside the
    // predicate reporting it, which is what makes a swapped label visible on
    // the page rather than only in a failing assertion (the same shape
    // `reject_wildcard_options` gives its options).
    let locks_and_for = [
        (!locks.is_empty(), "FOR UPDATE"),
        (for_clause.is_some(), "FOR"),
    ];
    if let Some((_, name)) = locks_and_for.iter().find(|(present, _)| *present) {
        return unsupported(name);
    }
    if settings.is_some() {
        return unsupported("SETTINGS");
    }
    if format_clause.is_some() {
        return unsupported("FORMAT");
    }
    if !pipe_operators.is_empty() {
        return unsupported("PIPE");
    }
    // Dead under GenericDialect (Redshift-only / BigQuery-only gates, and the
    // trait default for select_modifiers) — asserted as future-proofing so a
    // dialect change cannot open a silent hole.
    if exclude.is_some() {
        return unsupported("EXCLUDE");
    }
    if select_modifiers.is_some() {
        return unsupported("SELECT modifier");
    }
    if value_table_mode.is_some() {
        return unsupported("value table");
    }
    if *flavor != SelectFlavor::Standard {
        return unsupported("FROM-first SELECT");
    }
    Ok(())
}

/// A query must say what it projects. `supports_empty_projections()` is true,
/// so `SELECT FROM S3Object s` parses with an empty projection and would
/// serialize zero-column rows — harder to diagnose than a refusal.
fn reject_empty_projection(select: &Select) -> Result<(), Error> {
    if select.projection.is_empty() {
        return Err(Error::Parse("empty projection".into()));
    }
    Ok(())
}

/// `SelectItem::Wildcard` carries a `WildcardAdditionalOptions` that the
/// projection mapping drops. Rejected per option, by name, for the five
/// options `GenericDialect` can actually produce (`opt_alias` is dead — the
/// dialect does not override `supports_select_wildcard_with_alias`).
/// `QualifiedWildcard` carries the same struct but `s.*` is already refused
/// wholesale above.
///
/// No whole-struct equality against `Default` is used: the struct's `Default`
/// is a manual impl whose `wildcard_token` is a synthesized `Mul` token, so
/// equality against it never holds for a parsed wildcard.
fn reject_wildcard_options(select: &Select) -> Result<(), Error> {
    for item in &select.projection {
        let SelectItem::Wildcard(opts) = item else {
            continue;
        };
        // Homogeneous checks — all field presence — so they read as a table
        // and each name sits beside the predicate that reports it. That is
        // what makes a swapped label visible on the page rather than only in
        // a failing assertion.
        let options = [
            (opts.opt_ilike.is_some(), "ILIKE"),
            (opts.opt_exclude.is_some(), "EXCLUDE"),
            (opts.opt_except.is_some(), "EXCEPT"),
            (opts.opt_replace.is_some(), "REPLACE"),
            (opts.opt_rename.is_some(), "RENAME"),
        ];
        if let Some((_, name)) = options.iter().find(|(present, _)| *present) {
            return Err(Error::Unsupported((*name).into()));
        }
    }
    Ok(())
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

pub(crate) fn is_aggregate_name(name: &ObjectName) -> bool {
    let [ObjectNamePart::Identifier(part)] = name.0.as_slice() else {
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
    let [ObjectNamePart::Identifier(part)] = f.name.0.as_slice() else {
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

/// Walk every expression the plan carries and refuse any form the evaluator
/// cannot run. The walk is eager and whole-tree; `eval` is lazy and
/// short-circuiting, which is why the two cannot share a traversal — only
/// the per-node `unsupported_form` classification.
///
/// Entry points: each projection expression and `where_expr`. Aggregate
/// argument expressions are reached by recursion from the projection — no
/// separate traversal is needed (`pre_visit_expr` fires for nested
/// expressions too).
///
/// The sentinel and the function allowlist are the predicate's business:
/// this walk adds no list of its own, which is what keeps the parse-time
/// refusal and the runtime error from ever disagreeing. Nor does it add a
/// channel of its own: the predicate carries one per verdict, so a declined
/// form answers `Unsupported` and a malformed operand (`ESCAPE 'ab'`,
/// `1e999`) answers `Parse` — the same split `positive_limit` draws for
/// `LIMIT`, and the reason the walk's operand refusals need no wording here.
fn reject_unsupported_expressions(select: &Select, missing: &SentinelNames) -> Result<(), Error> {
    struct Walk<'a> {
        missing_name: &'a str,
    }
    impl Visitor for Walk<'_> {
        // The verdict travels in the break, so the visitor carries no second
        // piece of state that could fall out of step with it.
        type Break = Error;

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match engine::unsupported_form(expr, self.missing_name) {
                Some(form) => ControlFlow::Break(form.into()),
                None => ControlFlow::Continue(()),
            }
        }
    }
    // is_missing() returns an owned String — bind it before borrowing,
    // or the temporary does not live long enough for the `&'a str` field.
    let missing_name = missing.is_missing();
    let mut walk = Walk {
        missing_name: &missing_name,
    };
    let targets = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(e) => Some(e),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr),
            _ => None,
        })
        .chain(select.selection.iter());
    for expr in targets {
        if let ControlFlow::Break(err) = expr.visit(&mut walk) {
            return Err(err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{
        AccessExpr, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Subscript, UnaryOperator,
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
    fn long_custom_path_parses() {
        // Regression pin (2026-09-10): the FROM-factor span lookup used to
        // re-walk the input from byte 0 per token, making a long path
        // quadratic (tens of seconds at the 256 KiB cap). The monotonic
        // cursor makes this instant — no timing assertion, parsing at all
        // and with the full segment list is the contract.
        const SEGMENTS: usize = 4000;
        let mut sql = String::from("SELECT * FROM S3Object[*]");
        sql.push_str(&".a".repeat(SEGMENTS));
        let q = ok(&sql);
        assert_eq!(q.from.segments.len(), SEGMENTS + 1);
        assert_eq!(q.from.segments[0], PathSeg::Wild);
        assert_eq!(q.from.segments[SEGMENTS], PathSeg::Name("a".into()));
        assert_eq!(q.from.alias, None);
    }

    #[test]
    fn custom_path_after_newlines() {
        // The FROM-factor span cursor walks the whole statement, so the
        // leading lines (and the comments that carry them) must be accounted
        // for before the S3Object token. The path itself stays on one line —
        // the grammar allows no whitespace inside it.
        let q = ok("SELECT\n  x\nFROM\n  S3Object[*].b AS v");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("b".into())]
        );
        assert_eq!(q.from.alias.as_deref(), Some("v"));
        let q = ok("SELECT x /*\nfrom\n*/\nFROM S3Object[*].a");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("a".into())]
        );
        let q = ok("SELECT x -- c\nFROM S3Object[*].a");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("a".into())]
        );
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
        // `.name` without a first `[*]` is a segment the pest grammar refuses
        // (the path rule needs `[` first); the refused-continuation check
        // catches the dot → the "invalid FROM path" family.
        rej("SELECT * FROM S3Object.name", "invalid FROM path");
    }

    #[test]
    fn false_positive_projection_trigger_keeps_stock_behavior() {
        // `s.S3Object.name` in the projection triggers the skeleton; a
        // pathless factor must behave identically to stock.
        let q = ok("SELECT s.S3Object.name FROM S3Object s");
        assert_eq!(q.from.segments, Vec::new());
        assert_eq!(q.from.alias.as_deref(), Some("s"));
    }

    #[test]
    fn multi_statement_captured_slot_not_read() {
        // The second statement would overwrite the slot; the single-statement
        // rejection fires first, the slot is never read.
        rej(
            "SELECT * FROM S3Object[*].a; SELECT * FROM S3Object[*].b",
            "single statement",
        );
    }

    #[test]
    fn multi_statement_with_custom_path_candidate() {
        // The candidate check is whole-input by design (FIX A): the path in
        // statement 2 routes statement 1 (`SELECT 1`, no FROM) through the
        // skeleton, whose FROM expectation fails first. Multi-statement input
        // is rejected either way — the single-statement check would fire too
        // (`multi_statement_captured_slot_not_read`), just later.
        rej(
            "SELECT 1; SELECT * FROM S3Object[*].a",
            "invalid FROM: expected S3Object",
        );
        let e = parse("SELECT 1; SELECT * FROM S3Object[*].a").expect_err("must reject");
        assert!(!e.to_string().contains("single statement"), "{e}");
    }

    #[test]
    fn from_path_boundary_alignment_rejects_wider_word_class() {
        // Tokenizer's Word class is wider than pest `name` (non-ASCII, @, #):
        // pest stops mid-token, the walk consumes the whole token, and the
        // end-byte mismatch must reject — never truncate-accept `Name("foo")`.
        rej("SELECT * FROM S3Object[*].foo#bar", "invalid FROM path");
        rej("SELECT * FROM S3Object[*].café", "invalid FROM path");
    }

    #[test]
    fn s3object_root_index_is_refused() {
        // AWS: a path must start with `[*]` — the grammar's `first_seg` admits
        // only the wildcard at the root, so `S3Object[0]` never enters the
        // path rule and the refused-continuation check rejects the `[`
        // (end-to-end twin of path.rs's `no_index_first_segment`).
        rej("SELECT * FROM S3Object[0]", "invalid FROM path");
    }

    #[test]
    fn from_path_refused_continuation_family() {
        rej("SELECT * FROM S3Object.books", "invalid FROM path");
        rej("SELECT * FROM S3Object[0]", "invalid FROM path");
        rej("SELECT * FROM S3Object[*]['it''s']", "invalid FROM path");
        rej("SELECT * FROM S3Object[*].books[0x]", "invalid FROM path");
    }

    #[test]
    fn custom_path_into_is_fail_closed() {
        // INTO is not in the skeleton's clause order (projection → FROM):
        // the custom-factor statement is rejected closed rather than
        // silently ignoring the INTO clause (review 2026-09-09).
        rej(
            "SELECT s.a INTO t FROM S3Object[*].b s",
            "invalid FROM: expected S3Object",
        );
    }

    #[test]
    fn from_alias_keyword_rules() {
        // Clause keyword after AS is rejected (old behavior kept).
        rej("SELECT * FROM S3Object[*] AS where", "invalid FROM path");
        // A reserved keyword is never an alias (old pipeline rejected via
        // the stock tail; the skeleton rejects it itself).
        rej("SELECT * FROM S3Object[*] AS select", "invalid FROM path");
        // Quoted and bare aliases still work.
        let q = ok("SELECT * FROM S3Object[*] AS \"My Alias\"");
        assert_eq!(q.from.alias.as_deref(), Some("My Alias"));
        let q = ok("SELECT * FROM S3Object[*] s");
        assert_eq!(q.from.alias.as_deref(), Some("s"));
    }

    #[test]
    fn non_custom_alias_surface_is_stock() {
        // No custom FROM path → stock parse_optional_alias_inner rules apply:
        // any keyword after AS and quoted strings are aliases (2026-09-09
        // review; the deleted preprocess rejected these, stock accepts — no
        // old-behavior compatibility).
        let q = ok("SELECT * FROM S3Object AS where");
        assert_eq!(q.from.alias.as_deref(), Some("where"));
        let q = ok("SELECT * FROM S3Object AS 'x'");
        assert_eq!(q.from.alias.as_deref(), Some("x"));
    }

    #[test]
    fn limit_after_custom_factor_binds() {
        // The hand-rolled LIMIT clause on the custom path (stock's
        // parse_optional_limit_clause is private) binds like the stock one;
        // OFFSET and BY are parsed into the clause and rejected by the
        // validator's Unsupported("OFFSET")/Unsupported("LIMIT BY") arms —
        // the old pipeline's channel and message, from the one acceptance
        // gate (2026-09-10 simplify).
        let q = ok("SELECT s.a FROM S3Object[*] s WHERE s.b > 1 LIMIT 5");
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.from.segments, vec![PathSeg::Wild]);
        rej(
            "SELECT s.a FROM S3Object[*] s LIMIT 5 OFFSET 2",
            "unsupported: OFFSET",
        );
        rej(
            "SELECT s.a FROM S3Object[*] s LIMIT 5 BY s.a",
            "unsupported: LIMIT BY",
        );
    }

    #[test]
    fn distinct_before_offset_on_custom_path() {
        // The validator's first matching arm wins, and DISTINCT is checked
        // before the limit block — the ordering is shared with the stock path
        // (`reject_distinct` and `reject_offset` pin the same two arms), so
        // the custom path reports DISTINCT, not OFFSET.
        rej(
            "SELECT DISTINCT s.a FROM S3Object[*] s LIMIT 5 OFFSET 2",
            "unsupported: DISTINCT",
        );
        // Same query on the stock path: one validator, one arm order.
        rej(
            "SELECT DISTINCT s.a FROM S3Object s LIMIT 5 OFFSET 2",
            "unsupported: DISTINCT",
        );
    }

    #[test]
    fn is_missing_after_non_ascii_text() {
        // A multi-byte literal before the predicate must not misalign the
        // IS MISSING hook (the tokenizer's columns are character-based).
        // Pinned.
        let plan = parse("SELECT * FROM S3Object s WHERE s.a = 'ü' OR s.b IS MISSING").unwrap();
        assert!(plan.where_expr.is_some());
        // The hook is not wrapped in an error path: a sentinel call must
        // be present in the expression (the engine walks it).
        let s = plan.where_expr.unwrap().to_string();
        assert!(s.contains(&plan.missing.is_missing()), "{s}");
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
    fn user_sentinel_like_call_is_an_unknown_function() {
        // R15 guard deleted: the sentinel name is a per-request uuid, user
        // text cannot collide. A fixed-name `__s3_is_missing(x)` call is an
        // ordinary unknown function — and never adopted as the sentinel, so
        // the walk names it as an unknown function rather than letting it
        // mean `IS MISSING`.
        rej(
            "SELECT __s3_is_missing(s.a) FROM S3Object s",
            "unsupported: unsupported function: __s3_is_missing",
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
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), m),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
        // IS NOT MISSING = UnaryOp{Not} around the same sentinel (no second name).
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING");
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            }) => match expr.as_ref() {
                Expr::Function(f) => assert_eq!(f.name.to_string(), m),
                other => panic!("expected sentinel under NOT, got {other:?}"),
            },
            other => panic!("expected NOT-wrapped sentinel, got {other:?}"),
        }
        // Lowercase form: the hook compares NOT by keyword, not by text.
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x is not missing");
        assert!(matches!(q.where_expr, Some(Expr::UnaryOp { .. })));
    }

    #[test]
    fn is_missing_rewrite_quote_aware() {
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x = 'x IS MISSING y'");
        assert!(matches!(q.where_expr, Some(Expr::BinaryOp { .. })));
    }

    #[test]
    fn is_missing_in_projection_is_the_sentinel() {
        // Spec: IS MISSING in projection position (the other pins cover
        // WHERE). The projection item's expression is the sentinel Function
        // (the parse_infix hook runs in every expression position), and
        // IS NOT MISSING is UnaryOp{Not} wrapping the same sentinel.
        let q = ok("SELECT s.x IS MISSING FROM S3Object s");
        let m = q.missing.is_missing();
        match item(&q, 0) {
            Expr::Function(f) => {
                assert_eq!(f.name.to_string(), m);
            }
            other => panic!("expected missing sentinel in projection, got {other:?}"),
        }
        let q = ok("SELECT s.x IS NOT MISSING FROM S3Object s");
        let m = q.missing.is_missing();
        match item(&q, 0) {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => match expr.as_ref() {
                Expr::Function(f) => assert_eq!(f.name.to_string(), m),
                other => panic!("expected sentinel under NOT in projection, got {other:?}"),
            },
            other => panic!("expected NOT-wrapped sentinel in projection, got {other:?}"),
        }
    }

    #[test]
    fn is_missing_chained_forms_are_parse_errors() {
        // `x IS MISSING IS MISSING`: the hook's chained check sees the first
        // sentinel — bare Function, its own UnaryOp{Not} wrapper, or the
        // Nested (parenthesized) wrapper — and refuses; the chained form is
        // invalid SQL.
        rej(
            "SELECT s.x FROM S3Object s WHERE s.x IS MISSING IS MISSING",
            "invalid IS MISSING expression",
        );
        rej(
            "SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING IS MISSING",
            "invalid IS MISSING expression",
        );
        // Parenthesized chains: the inner hook production is wrapped in our
        // own Expr::Nested — the chained check recurses through it and
        // refuses (never wrapping Nested(sentinel) into a second call).
        rej(
            "SELECT s.x FROM S3Object s WHERE (s.x IS MISSING) IS MISSING",
            "invalid IS MISSING expression",
        );
        rej(
            "SELECT s.x FROM S3Object s WHERE (s.x IS MISSING) IS NOT MISSING",
            "invalid IS MISSING expression",
        );
        // The legit single form still rewrites.
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS MISSING");
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), m),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn traversal_plus_missing_rewrite() {
        // The custom skeleton's parse_expr (WHERE clause) runs the dialect's
        // parse_infix hook, so the IS MISSING sentinel is produced on the
        // custom path exactly as on the stock path.
        let q = ok("SELECT s.x FROM S3Object[*].books[*].price s WHERE s.x IS MISSING");
        assert_eq!(q.from.segments.last(), Some(&PathSeg::Name("price".into())));
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::Function(f)) => {
                assert_eq!(f.name.to_string(), m);
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
    fn delegation_drift_surface() {
        // Drift-sensitive: these validator messages appear only after a successful
        // stock parse — a missed supports_* delegation makes the parse stage fail
        // with a different error, so the assert fails red. Channel-only asserts are
        // vacuous (both states Err).
        rej(
            "SELECT * FROM S3Object s LIMIT 3, 5",
            "LIMIT must be a positive integer", // supports_limit_comma → OffsetCommaLimit arm
        );
        rej(
            "SELECT * FROM S3Object s LIMIT 5 BY s.a",
            "unsupported: LIMIT BY", // supports_limit_by → LimitOffset limit_by arm
        );
        rej(
            "SELECT s.a FROM S3Object s GROUP BY GROUPING SETS ((s.a))",
            "unsupported: GROUP BY", // supports_group_by_expr true arm consumes GROUPING SETS
        );
        // supports_select_wildcard_except: the EXCEPT parses into the
        // Wildcard item's options, which `reject_wildcard_options` now
        // refuses by name (before that check existed the projection loop
        // dropped them and the plan was the plain `*`). Still
        // drift-sensitive: a delegation miss leaves the stock parse
        // failing at EXCEPT with a syntax error ("…in the query body,
        // found: a"), so it is the message that discriminates, not the
        // bare Err channel.
        rej("SELECT * EXCEPT (a) FROM S3Object s", "unsupported: EXCEPT");
        // supports_parens_around_table_factor: with the argument true the
        // stock factor parser unwraps `(S3Object s)` into the plain factor
        // (+alias); false would parse a derived table and fail the
        // validator's Table arm. The old byte scanner rejected the parens
        // pre-parse ("invalid FROM: expected S3Object" — old channel
        // Parse), so this row is documented drift: old Parse, new Ok.
        let q = ok("SELECT * FROM (S3Object s)");
        assert_eq!(q.from.segments, Vec::<PathSeg>::new());
        assert_eq!(q.from.alias.as_deref(), Some("s"));
        ok("SELECT * FROM S3Object s WHERE s.x = 1"); // stock baseline
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
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), m),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
    }

    #[test]
    fn from_scan_is_comment_aware() {
        // A `-- from` line comment before the real FROM must not misdirect
        // the custom-path probe (peek_nth_token skips whitespace, comments
        // included): the comment's `from` never looks like the FROM factor
        // and the statement goes down the stock path.
        let q = ok("SELECT x -- from table\nFROM S3Object s");
        assert_eq!(q.projections.len(), 1);
        // A newline inside a block comment puts `from` at a word boundary —
        // the probe skips the comment there too.
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
        // Stock 0.62 never parses `from` here as a bare column: GenericDialect
        // supports_empty_projections matches peek_keyword(FROM), so the word
        // after SELECT is consumed as the FROM keyword, the next word
        // becomes the table factor, and `s` is left over — a stock
        // end-of-statement error. The honest rejection is a syntax error
        // (never the old byte scanner's "invalid FROM: expected S3Object",
        // never a panic or an Unsupported channel). The honest ways to name
        // a column `from` are `s.from` and `"from"`.
        rej("SELECT from FROM S3Object s", "end of statement");
        // Custom-path variant: the skeleton calls parse_projection()
        // directly, so stock parse_select's empty-projection arm (which
        // lives in parse_select — peek_keyword(FROM) before parse_projection,
        // parser/mod.rs:14743) never fires; the rejection is
        // parse_projection's own "Expected an expression" for the `from`
        // word. Same Parse channel as the no-path variant, different
        // message — on purpose (arm location, review R4).
        rej("SELECT from FROM S3Object[*].a", "Expected an expression");
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
        // validator's JOIN arm (no custom probe hit: the path never
        // declines stock parsing with a JOIN behind a plain factor).
        rej(
            "SELECT * FROM S3Object s JOIN t ON s.x = t.x",
            "unsupported: JOIN",
        );
    }

    #[test]
    fn custom_path_join_family_pre_rejected() {
        // Every word in is_join_keyword's list, placed as the token right
        // after the custom factor's alias — exactly where the skeleton's
        // check peeks. The old pipeline surfaced a join behind the custom
        // factor as Unsupported("JOIN") (splice → stock parse → validator);
        // the skeleton rejects with the same channel and message (restored
        // 2026-09-10 through the unsupported marker). ON/USING need no
        // preceding join context: the check peeks the immediate next token,
        // so `… s ON t` is caught the same way.
        for kw in [
            "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "NATURAL", "ON", "USING",
        ] {
            let sql = format!("SELECT s.a FROM S3Object[*].b s {kw} t");
            rej(&sql, "unsupported: JOIN");
            let e = parse(&sql).expect_err("custom-path JOIN-family must reject");
            assert!(
                matches!(&e, Error::Unsupported(k) if k == "JOIN"),
                "{sql}: got {e:?}"
            );
        }
    }

    #[test]
    fn join_keywords_are_clause_keywords() {
        // Drift guard: the dialect's JOIN-family rejection list and the alias
        // clause-keyword set share one authority — every JOIN keyword must
        // also be a clause keyword (both refuse a word in alias position).
        for kw in JOIN_KEYWORDS {
            assert!(
                CLAUSE_KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(kw)),
                "{kw} missing from CLAUSE_KEYWORDS"
            );
        }
    }

    #[test]
    fn quoted_ident_doubling_survives_scan() {
        // Quoted identifiers and strings tokenize cleanly down the stock
        // path — a `from` word inside a quote must not look like the FROM
        // factor (the tokenizer owns quote state, not the probe).
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
        // The unterminated quote dies in parse_sql's upfront tokenization
        // (before any statement hook runs); the rejection channel is the
        // tokenizer's, not the pest one — pinned here.
        rej(
            "SELECT * FROM S3Object[*].'books",
            "Unterminated string literal",
        );
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

    // ------------------------------------------------------------------
    // Parity harness (review R7). The OLD pipeline — git show
    // HEAD:crates/tinio-select/src/sql.rs at 03109b2 — ran
    // reject_reserved_functions → preprocess_from (byte-scanner FROM
    // splice) → rewrite_is_missing (text splice) → stock GenericDialect
    // parse → the same validator arms the new pipeline keeps. Each row
    // asserts what the NEW pipeline produces; the comment records the
    // OLD channel (modeled from the old source). Rows without a drift
    // marker assert the channel matches the old pipeline's — a missed
    // Ok<->Err side flip is a regression and fails red. The custom-path
    // LIMIT BY / OFFSET rows match without a drift marker too: the
    // skeleton parses those constructs into the clause and the validator's
    // own Unsupported("LIMIT BY")/Unsupported("OFFSET") arms reject them
    // (2026-09-10 simplify — one acceptance gate); the custom-path JOIN row
    // keeps the old Unsupported channel through the skeleton's unsupported
    // marker (the skeleton parses no joins). FailClosed is the INTO
    // row (old pipeline silently ignored INTO, the skeleton rejects
    // closed — its own pin is custom_path_into_is_fail_closed).
    // ------------------------------------------------------------------
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Old {
        Accepted,
        Unsupported,
        Parse,
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Drift {
        None,
        FailClosed,
    }

    struct Row {
        sql: &'static str,
        old: Old,
        drift: Drift,
        /// Expected message fragment for the new pipeline's rejection
        /// (empty when the case is accepted).
        contains: &'static str,
    }

    #[test]
    fn parity_harness_old_pipeline_channels() {
        let rows = [
            // --- accepted, stock or spliced: old Accept → new Accept ---
            Row {
                sql: "SELECT * FROM S3Object s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.a FROM S3Object[*] s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT price FROM S3Object[*].books[*].price",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT * FROM S3Object[*]['a b']",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT * FROM S3Object[*].books[0]",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT * FROM S3Object[*].*",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.a FROM S3Object[*].b s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.a FROM S3Object[*].b AS v",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.x FROM S3Object s WHERE s.x IS MISSING",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.x IS MISSING FROM S3Object s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT count(*) FROM S3Object s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s LIMIT 5",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            Row {
                sql: "SELECT x FROM S3Object[*].limit.name s",
                old: Old::Accepted,
                drift: Drift::None,
                contains: "",
            },
            // --- rejected, same channel both pipelines ---
            Row {
                sql: "SELECT s.x FROM S3Object s WHERE s.x IS MISSING IS MISSING",
                old: Old::Parse,
                drift: Drift::None,
                contains: "invalid IS MISSING expression",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s LIMIT 5 OFFSET 2",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: OFFSET",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s LIMIT 5 BY s.a",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: LIMIT BY",
            },
            Row {
                sql: "SELECT * FROM S3Object s JOIN t ON s.x = t.x",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: JOIN",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s GROUP BY s.b",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: GROUP BY",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s ORDER BY s.b",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: ORDER BY",
            },
            Row {
                sql: "SELECT DISTINCT s.a FROM S3Object s",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: DISTINCT",
            },
            Row {
                sql: "SELECT s.a FROM S3Object s UNION SELECT s.b FROM S3Object s",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: UNION",
            },
            Row {
                sql: "SELECT * FROM S3Object[*].a UNION SELECT * FROM b",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: UNION",
            },
            Row {
                sql: "SELECT * FROM (SELECT * FROM S3Object s)",
                old: Old::Parse,
                drift: Drift::None,
                contains: "FROM must reference exactly one S3Object",
            },
            Row {
                sql: "SELECT from FROM S3Object s",
                old: Old::Parse,
                drift: Drift::None,
                contains: "end of statement",
            },
            Row {
                sql: "SELECT from FROM S3Object[*].a",
                old: Old::Parse,
                drift: Drift::None,
                contains: "Expected an expression",
            },
            Row {
                sql: "SELECT * FROM other",
                old: Old::Parse,
                drift: Drift::None,
                contains: "invalid FROM: expected S3Object",
            },
            // --- custom-path rejections: same channel both pipelines:
            // LIMIT BY / OFFSET through the validator's arms (the skeleton
            // parses them into the clause — 2026-09-10 simplify), JOIN
            // through the skeleton's unsupported marker (channel restored
            // 2026-09-10) ---
            Row {
                sql: "SELECT s.a FROM S3Object[*].b s LIMIT 5 BY s.a",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: LIMIT BY",
            },
            Row {
                sql: "SELECT s.a FROM S3Object[*].b s LIMIT 5 OFFSET 2",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: OFFSET",
            },
            Row {
                sql: "SELECT * FROM S3Object[*].a s JOIN t ON s.x = t.x",
                old: Old::Unsupported,
                drift: Drift::None,
                contains: "unsupported: JOIN",
            },
            // --- documented channel drift ---
            // old Accepted (the old pipeline ignored INTO — its validator
            // had no into arm); new Parse, fail closed (R4 kept pin).
            Row {
                sql: "SELECT s.a INTO t FROM S3Object[*].b s",
                old: Old::Accepted,
                drift: Drift::FailClosed,
                contains: "invalid FROM: expected S3Object",
            },
        ];
        for row in &rows {
            let (got, msg) = match parse(row.sql) {
                Ok(_) => (Old::Accepted, String::new()),
                // Assert on the full Display (the Unsupported payload is
                // the fragment after "unsupported: "; Parse carries the
                // whole "syntax error: ..." text already).
                Err(e) => {
                    let s = e.to_string();
                    let c = match &e {
                        Error::Parse(_) => Old::Parse,
                        Error::Unsupported(_) => Old::Unsupported,
                        other => panic!("{}: unexpected error channel {other:?}", row.sql),
                    };
                    (c, s)
                }
            };
            let expected = match row.drift {
                // For a non-drift row this IS the old channel: the equality
                // below pins the side (Err stays Err) and the channel in one
                // assert — a flip to Ok or another channel fails red.
                Drift::None => row.old,
                // The INTO row: assert the new channel (the old one is
                // documented in the comment above the row).
                Drift::FailClosed => Old::Parse,
            };
            assert_eq!(
                got, expected,
                "{}: new={got:?} expected={expected:?} old={:?}",
                row.sql, row.old
            );
            if expected != Old::Accepted {
                assert!(
                    msg.contains(row.contains),
                    "{}: {msg} does not contain {:?}",
                    row.sql,
                    row.contains
                );
            }
        }
        // Plan shapes for the accepted rows that no dedicated test pins: the
        // quoted-name / index / `.*` segments, the `AS v` factor and the
        // keyword-named path. The other accepted rows' shapes are pinned by
        // `wildcard_where_limit` (`* FROM S3Object s`: empty segments, alias
        // and the Wild projection), `from_alias_keyword_rules` (the
        // `S3Object[*] s` bare alias), `traversal_path` (the four-segment
        // walk + projection), `is_missing_rewrite` and
        // `is_missing_in_projection_is_the_sentinel` (the sentinel shapes),
        // `count_star_is_aggregate` and `limit_after_custom_factor_binds`
        // (`S3Object[*] s` segments + the LIMIT binding). The old pipeline
        // produced the same shapes on the spliced text; the MISSING sentinel
        // names are the per-request uuid now instead of the old fixed pair.
        let q = ok("SELECT s.a FROM S3Object[*] s");
        assert_eq!(q.projections.len(), 1);
        let q = ok("SELECT * FROM S3Object[*]['a b']");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("a b".into())]
        );
        let q = ok("SELECT * FROM S3Object[*].books[0]");
        assert_eq!(
            q.from.segments,
            vec![
                PathSeg::Wild,
                PathSeg::Name("books".into()),
                PathSeg::Index(0)
            ]
        );
        let q = ok("SELECT * FROM S3Object[*].*");
        assert_eq!(q.from.segments, vec![PathSeg::Wild, PathSeg::Wild]);
        let q = ok("SELECT s.a FROM S3Object[*].b AS v");
        assert_eq!(
            q.from.segments,
            vec![PathSeg::Wild, PathSeg::Name("b".into())]
        );
        assert_eq!(q.from.alias.as_deref(), Some("v"));
        let q = ok("SELECT x FROM S3Object[*].limit.name s");
        assert_eq!(
            q.from.segments,
            vec![
                PathSeg::Wild,
                PathSeg::Name("limit".into()),
                PathSeg::Name("name".into())
            ]
        );
    }

    #[test]
    fn unread_clauses_are_rejected_on_both_from_paths() {
        // Each construct in BOTH spellings. The custom path may fail earlier
        // (clause ordering) with a different message — both must be rejected;
        // the messages are never compared across paths (spec: legitimately
        // different, and the custom one is known to be less precise).
        //
        // Observed: EVERY custom spelling here fails earlier in the skeleton,
        // so none of the new arms is reached on that path. The skeleton's
        // clause order is DISTINCT → projection → FROM → WHERE → GROUP BY →
        // HAVING → ORDER BY → LIMIT, so any clause outside it is left as a
        // trailing token (`Expected: end of statement, found: …`) — all but
        // the first two rows, which fail on the FROM keyword check itself
        // (`invalid FROM: expected S3Object`) because the skeleton parses no
        // TOP and no INTO.
        //
        // The stock path's message is pinned per row (its own name, not just
        // *a* refusal): the deliverable is that each refused clause reports
        // its own name, and a swapped label between two arms of one `if`
        // chain is otherwise invisible. The custom path keeps `is_err()` —
        // its message is the skeleton's, and asserting one here would pin
        // clause ordering instead of the name.
        let stock = [
            ("SELECT TOP 1 s.a FROM S3Object s", "TOP"),
            ("SELECT s.a INTO t FROM S3Object s", "INTO"),
            ("SELECT s.a FROM S3Object s QUALIFY s.a > 1", "QUALIFY"),
            (
                "SELECT s.a FROM S3Object s WINDOW w AS (PARTITION BY s.a)",
                "WINDOW",
            ),
            (
                "SELECT s.a FROM S3Object s FETCH FIRST 1 ROWS ONLY",
                "FETCH",
            ),
            ("SELECT s.a FROM S3Object s FOR UPDATE", "FOR UPDATE"),
            ("SELECT s.a FROM S3Object s FOR JSON AUTO", "FOR"),
            ("SELECT s.a FROM S3Object s SETTINGS x = 1", "SETTINGS"),
            ("SELECT s.a FROM S3Object s FORMAT CSV", "FORMAT"),
            (
                "SELECT s.a FROM S3Object s LATERAL VIEW explode(s.b) t AS c",
                "LATERAL VIEW",
            ),
            ("SELECT s.a FROM S3Object s PREWHERE s.a > 1", "PREWHERE"),
            (
                "SELECT s.a FROM S3Object s CONNECT BY PRIOR s.a = s.b",
                "CONNECT BY",
            ),
            ("SELECT s.a FROM S3Object s CLUSTER BY s.a", "CLUSTER BY"),
            (
                "SELECT s.a FROM S3Object s DISTRIBUTE BY s.a",
                "DISTRIBUTE BY",
            ),
            ("SELECT s.a FROM S3Object s SORT BY s.a", "SORT BY"),
            // `supports_pipe_operator` is delegated `true`, so `|>` parses
            // into `Query.pipe_operators` and only the PIPE arm refuses it.
            ("SELECT s.a FROM S3Object s |> SELECT s.b", "PIPE"),
        ];
        for (q, name) in stock {
            rej(q, &format!("S3 select: unsupported: {name}"));
            assert!(
                parse(&q.replace("S3Object s", "S3Object[*] s")).is_err(),
                "must reject the custom spelling of: {q}"
            );
        }
    }

    #[test]
    fn from_factor_fields_must_be_default() {
        // The FROM factor carries ten fields; `FromClause` keeps two. Anything
        // else is dropped on the floor, so each must be refused by name rather
        // than parsed and ignored — the `..` in `validate_from` swallowed all
        // of them, which is how `PARTITION (p)` came to mean a bare scan.
        // Four of these predate the dialect change that surfaced the fifth;
        // they are the same silent-drop defect, fixed in the same arm. The
        // last row is the one field of the class that is not a `TableFactor`
        // field: the alias's own column list, which `FromClause` dropped just
        // as silently (probe-verified 2026-09-11).
        for (q, name) in [
            ("SELECT * FROM S3Object PARTITION (p)", "PARTITION"),
            ("SELECT * FROM S3Object(p)", "table-function args"),
            ("SELECT * FROM S3Object WITH (a)", "WITH"),
            ("SELECT * FROM S3Object TABLESAMPLE (10)", "TABLESAMPLE"),
            ("SELECT * FROM S3Object s (a, b)", "alias column list"),
        ] {
            assert_eq!(
                parse(q).unwrap_err().to_string(),
                format!("S3 select: unsupported: {name}"),
                "for {q}"
            );
        }
        // The control: an ordinary factor still parses.
        assert!(parse("SELECT * FROM S3Object s").is_ok());
    }

    #[test]
    fn from_first_select_is_rejected() {
        // `SelectFlavor::FromFirst` and `::FromFirstNoSelect` — both reachable
        // because GenericDialect::supports_from_first_select() is true. The
        // message is pinned, not just the channel: a bare `is_err()` cannot
        // tell this refusal from any other rejection of the same query.
        for q in ["FROM S3Object s SELECT s.a", "FROM S3Object s"] {
            assert_eq!(
                parse(q).unwrap_err().to_string(),
                "S3 select: unsupported: FROM-first SELECT",
                "for {q}"
            );
        }
    }

    #[test]
    fn empty_projection_is_rejected() {
        // GenericDialect::supports_empty_projections() is true.
        assert_eq!(
            parse("SELECT FROM S3Object s").unwrap_err().to_string(),
            "S3 select: empty projection"
        );
        // The custom FROM path cannot produce one at all: its skeleton parses
        // `FROM S3Object…` inside `parse_statement`, so `SELECT FROM` dies at
        // the parser before `reject_empty_projection` could run — a syntax
        // error, not the validator's message. Pinned so the second spelling
        // stays explicitly covered rather than silently assumed (design: the
        // gap is either closed by the stock arm or pinned here).
        for q in ["SELECT FROM S3Object[*] s", "SELECT FROM S3Object[*].a.b s"] {
            let err = parse(q).unwrap_err();
            assert!(
                matches!(err, Error::Parse(_)),
                "must be a syntax error, got {err:?} for {q}"
            );
            assert!(
                err.to_string().contains("syntax error"),
                "must be the parser's refusal, got {err} for {q}"
            );
        }
    }

    #[test]
    fn ordinary_queries_still_parse() {
        // The guard against the flavor assertion rejecting everything.
        for q in [
            "SELECT s.a FROM S3Object s",
            "SELECT * FROM S3Object s",
            "SELECT count(*) FROM S3Object s",
            "SELECT s.a, s.b FROM S3Object s WHERE s.a > 1 LIMIT 10",
            "SELECT s.a FROM S3Object[*].books[0] s",
            "SELECT s.a FROM s3object s",
        ] {
            assert!(parse(q).is_ok(), "must accept: {q}");
        }
    }

    #[test]
    fn optimizer_hints_are_accepted_and_ignored() {
        // The one named exception: a hint is advisory, dropping it is correct.
        // Pinned so it cannot be "fixed" into a rejection later.
        assert!(parse("SELECT /*+ hint */ * FROM S3Object s").is_ok());
    }

    #[test]
    fn wildcard_options_are_rejected_by_name() {
        // The projection mapping turns `SelectItem::Wildcard` into
        // `Projection::Wild` and drops the options — so `SELECT * EXCLUDE (a)`
        // returns the very column the user asked to withhold.
        let cases = [
            ("SELECT * EXCLUDE (a) FROM S3Object s", "EXCLUDE"),
            ("SELECT * EXCEPT (a) FROM S3Object s", "EXCEPT"),
            ("SELECT * REPLACE (a AS b) FROM S3Object s", "REPLACE"),
            ("SELECT * RENAME (a AS b) FROM S3Object s", "RENAME"),
            ("SELECT * ILIKE '%a%' FROM S3Object s", "ILIKE"),
        ];
        for (q, name) in cases {
            assert_eq!(
                parse(q).unwrap_err().to_string(),
                format!("S3 select: unsupported: {name}"),
                "for {q}"
            );
        }
    }

    #[test]
    fn wildcard_options_are_rejected_on_the_custom_path_too() {
        // The custom skeleton ingests the options with the projection, so this
        // check is NOT a no-op there (probe-verified 2026-09-10) — and unlike
        // the clause arms, the custom path reaches THIS arm and so reports the
        // same name the stock path does. Pinned to the sibling's strength.
        for (q, name) in [
            ("SELECT * EXCLUDE (a) FROM S3Object[*] s", "EXCLUDE"),
            ("SELECT * EXCEPT (a) FROM S3Object[*] s", "EXCEPT"),
        ] {
            rej(q, &format!("S3 select: unsupported: {name}"));
        }
    }

    #[test]
    fn plain_wildcards_still_parse() {
        for q in ["SELECT * FROM S3Object s", "SELECT s.a, * FROM S3Object s"] {
            assert!(parse(q).is_ok(), "must accept: {q}");
        }
    }

    #[test]
    fn wildcard_alias_option_stays_unreachable() {
        // `WildcardAdditionalOptions::opt_alias` has no arm in
        // `reject_wildcard_options`, because this dialect never sets it:
        // sqlparser gates `SELECT * AS x` on `supports_select_wildcard_with_alias`,
        // which `S3SelectDialect` does not delegate, so it takes the trait
        // default (`false`).
        //
        // Pinned because it is the one wildcard option the check would
        // silently drop if a sqlparser bump — or a new delegation — enabled
        // it. This test goes red first, which is the prompt to add the arm.
        assert!(parse("SELECT * AS x FROM S3Object s").is_err());
    }

    #[test]
    fn the_walk_runs_before_the_aggregate_shape_check() {
        // The walk descends into an aggregate's argument, so an unsupported
        // form nested there is refused at request level naming the form.
        assert_eq!(
            parse("SELECT count(LOWER(s.a)) FROM S3Object s")
                .unwrap_err()
                .to_string(),
            "S3 select: unsupported: unsupported function: LOWER"
        );
        // Q7's ordering guard. `count(*) + LOWER(s.a)` trips BOTH checks: the
        // projection is a `BinaryOp`, so `contains_aggregate` is true and
        // `validate_aggregate_item` rejects it as a non-bare call. Only a walk
        // that runs *first* yields the LOWER message — move the call below the
        // aggregate checks and this assertion fails.
        assert_eq!(
            parse("SELECT count(*) + LOWER(s.a) FROM S3Object s")
                .unwrap_err()
                .to_string(),
            "S3 select: unsupported: unsupported function: LOWER"
        );
        // The contrasting half of the guard: the same projection shape with a
        // supported operand still gets the aggregate-shape diagnosis, so the
        // pair above contrasts two live paths rather than a message that no
        // longer exists.
        assert_eq!(
            parse("SELECT count(*) + s.a FROM S3Object s")
                .unwrap_err()
                .to_string(),
            "S3 select: non-aggregate expression in aggregate select list"
        );
    }

    #[test]
    fn aggregates_and_missing_predicates_still_parse() {
        // The sentinel exception: the dialect rewrites `IS [NOT] MISSING` into a
        // Function node named __s3_is_missing_<uuid>, minted per request. Failing
        // to admit it turns every such query into a 400.
        for q in [
            "SELECT count(*) FROM S3Object s",
            "SELECT count(s._1), sum(s.x) FROM S3Object s WHERE s.y IS MISSING",
            "SELECT s.a FROM S3Object s WHERE s.a IS NOT MISSING",
            "SELECT s.a FROM S3Object s WHERE s.a is not missing",
            "SELECT s.a FROM S3Object s WHERE (s.a) IS MISSING",
            "SELECT s.a FROM S3Object s WHERE s.a IS NULL",
            "SELECT s.a FROM S3Object s WHERE s.a IN (1, 2)",
            "SELECT s.a FROM S3Object s WHERE s.a BETWEEN 1 AND 2",
            "SELECT s.a FROM S3Object s WHERE s.a NOT LIKE 'x%' ESCAPE '!'",
        ] {
            assert!(parse(q).is_ok(), "must accept: {q}");
        }
    }

    #[test]
    fn keyword_shaped_identifiers_are_not_expressions() {
        // Form-based, never text-based: the recorded AWS divergence (spec Q2/Q8).
        for q in [
            "SELECT s.CAST FROM S3Object s",
            "SELECT s.date FROM S3Object s",
        ] {
            assert!(parse(q).is_ok(), "must accept: {q}");
        }
    }

    #[test]
    fn wildcard_options_inside_a_function_argument_are_refused() {
        // Task 3 review: `foo` is not an aggregate, so `aggregates` is false
        // and `validate_aggregate_item` never inspects the argument — the
        // options inside it were silently dropped. The walk closes the hole by
        // classifying the enclosing `Expr::Function`, so the diagnostic names
        // the *function*, not EXCLUDE. The guarantee is the refusal; the
        // message is incidental.
        rej(
            "SELECT foo(* EXCLUDE (a)) FROM S3Object s",
            "unsupported function: foo",
        );
        // The allowed-name variant never reached the drop: a wildcard carrying
        // options is not a bare aggregate argument, so the shape check refuses
        // it without the walk.
        rej(
            "SELECT count(* EXCLUDE (a)) FROM S3Object s",
            "non-aggregate expression in aggregate select list",
        );
    }
}
