//! Engine: evaluator, WHERE / projection / LIMIT per record.
//!
//! `Engine` consumes one `Record` at a time; `next` filters by the plan's
//! WHERE (TRUE only), evaluates the projection, and caps emitted rows at
//! LIMIT. `finish` carries the aggregate row (Task 7) — for the
//! non-aggregate plans this task serves it returns `Ok(None)`.
//!
//! MISSING is a `Field` concept, not a `Value` one: `eval` collapses a
//! missing column to `Value::Null`, and the three MISSING-sensitive spots
//! (`IS NULL`[...], the per-request MISSING sentinel) resolve their operand
//! at the `Field` level first so a present-but-null value stays distinct
//! from an absent field. The `IsNot*` forms are the pure negations of their
//! cousins, so a MISSING operand answers `true` there (pinned, review
//! 2026-09-06b R12).

use std::{borrow::Cow, cell::RefCell, cmp::Ordering, mem};

use parse_display::Display;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sqlparser::{
    ast,
    ast::{
        AccessExpr, BinaryOperator, CastKind, DuplicateTreatment, Expr, Function, FunctionArg,
        FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart, Subscript,
        UnaryOperator, Value as AstValue, ValueWithSpan,
    },
};

use crate::{
    error::Error,
    json::{json_lookup, to_value},
    row::{Field, NameStyle, Record, Value, display, parse_number},
    sql::{Projection, QueryPlan, contains_aggregate, is_aggregate_name},
};

/// One output row: keys (alias > plain field name > the record's names) and
/// the projected values (a filtered row never reaches here).
#[derive(Debug, Clone, PartialEq)]
pub struct OutRow {
    pub keys: Vec<String>,
    pub vals: Vec<Field>,
}

/// Plan-driven evaluator: WHERE filter, projection, LIMIT; aggregate mode
/// (Task 7) accumulates instead, producing its row in `finish`.
pub struct Engine {
    plan: QueryPlan,
    /// Rows emitted so far (the LIMIT counter).
    emitted: usize,
    /// Aggregate accumulator: `None` for non-aggregate plans.
    agg_state: Option<AggState>,
    /// Per-projection kind, classified once on the first accumulated row —
    /// the parse layer already guarantees the bare-call shape, so this
    /// memo is also the engine's defense.
    agg_kinds: Option<Vec<AggKind>>,
    /// Precomputed `Item` projection keys (alias, else plain field name,
    /// else the expression text). The expression-text fallback re-renders
    /// the whole AST, so building it per row was pure waste (review
    /// 2026-09-06 simplify); `None` at `Wild` slots.
    item_keys: Vec<Option<String>>,
    /// This request's MISSING sentinel name, derived once in `new`
    /// (`SentinelNames::is_missing()` formats a fresh String — paying it
    /// per row on every Function eval was the waste; review 2026-09-09).
    missing_name: String,
}

/// One projection item's aggregate form.
#[derive(Debug, Clone, PartialEq)]
enum AggKind {
    CountStar,
    Count(Expr),
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
}

/// The running aggregate accumulator: `count` = rows seen (COUNT(*)),
/// `per_col[i]` = the i-th projection's column state.
#[derive(Debug, Default)]
struct AggState {
    count: u64,
    per_col: Vec<AggCol>,
}

/// One aggregate column's running state: contributing-value count, the
/// running sum, and the MIN/MAX extrema — typed by the first contributing
/// value.
#[derive(Debug, Clone, Default)]
struct AggCol {
    count: u64,
    sum: Option<Decimal>,
    min: Option<Value>,
    max: Option<Value>,
    /// MIN/MAX comparison spine, fixed by the first contributor.
    extrema: Option<ExtremaMode>,
}

/// The MIN/MAX spine: numeric when the first contributing value parses,
/// strict-lexicographic string when it does not.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExtremaMode {
    Numeric,
    String,
}

/// What an expression in flight sees: the record under test plus the FROM
/// alias (the JSON path/access arms need the alias; flat lookups do not),
/// plus this request's MISSING sentinel name (the per-request uuid name —
/// the dialect hook produced the sentinel call under it; derived once in
/// `Engine::new`, never per row).
struct RowCtx<'a> {
    record: &'a Record,
    alias: &'a Option<String>,
    missing_name: &'a str,
}

impl Engine {
    pub fn new(plan: QueryPlan) -> Self {
        let missing_name = plan.missing.is_missing();
        let agg_state = plan.aggregates.then(|| AggState {
            count: 0,
            per_col: vec![AggCol::default(); plan.projections.len()],
        });
        let item_keys = plan
            .projections
            .iter()
            .map(|item| match item {
                Projection::Item { expr, alias } => {
                    Some(alias.clone().unwrap_or_else(|| plain_key(expr)))
                }
                Projection::Wild => None,
            })
            .collect();
        Self {
            plan,
            emitted: 0,
            agg_state,
            agg_kinds: None,
            item_keys,
            missing_name,
        }
    }

    /// One record through the plan. `Ok(None)` = filtered, or LIMIT reached;
    /// in aggregate mode it always accumulates and returns `Ok(None)` (the
    /// single row is live in `finish`).
    pub fn next(&mut self, rec: Record) -> Result<Option<OutRow>, Error> {
        if self.agg_state.is_some() {
            return self.aggregate_next(rec);
        }
        if self.emitted >= self.plan.limit.unwrap_or(usize::MAX) {
            return Ok(None);
        }
        let ctx = RowCtx {
            record: &rec,
            alias: &self.plan.from.alias,
            missing_name: &self.missing_name,
        };
        if !self.passes(&ctx)? {
            return Ok(None);
        }
        let row = self.project(&ctx)?;
        self.emitted += 1;
        Ok(Some(row))
    }

    /// Non-aggregate plans: end-of-stream, `Ok(None)` — matching `next`'s
    /// row flow. Aggregate mode (Task 7): the caller loops `next` to EOF,
    /// then `finish` produces the single aggregate row; a repeated call
    /// returns `Ok(None)` (the state is consumed).
    pub fn finish(&mut self) -> Result<Option<OutRow>, Error> {
        let Some(state) = self.agg_state.take() else {
            return Ok(None);
        };
        self.classified()?;
        let kinds = self.agg_kinds.as_deref().expect("classified above");
        let mut keys = Vec::with_capacity(self.plan.projections.len());
        let mut vals = Vec::with_capacity(self.plan.projections.len());
        for (item, (kind, col)) in self
            .plan
            .projections
            .iter()
            .zip(kinds.iter().zip(state.per_col.iter()))
        {
            let Projection::Item { expr, alias } = item else {
                unreachable!("aggregate plans never carry Wild (rejected at parse)");
            };
            keys.push(alias.clone().unwrap_or_else(|| plain_key(expr)));
            vals.push(Field::Present(finish_value(kind, &state, col)?));
        }
        Ok(Some(OutRow { keys, vals }))
    }

    /// Aggregate mode: WHERE-filter, then accumulate for the row. A filtered
    /// record contributes nothing; LIMIT applies to the single output row
    /// (never a positive bound) and is not consulted here.
    fn aggregate_next(&mut self, rec: Record) -> Result<Option<OutRow>, Error> {
        self.classified()?;
        let ctx = RowCtx {
            record: &rec,
            alias: &self.plan.from.alias,
            missing_name: &self.missing_name,
        };
        if self.passes(&ctx)? {
            let kinds = self.agg_kinds.as_deref().expect("classified above");
            let state = self.agg_state.as_mut().expect("aggregate mode");
            accumulate(kinds, state, &ctx)?;
        }
        Ok(None)
    }

    /// WHERE filter: a record passes only when the predicate evaluates to
    /// TRUE (SQL 3VL — Null/MISSING and non-boolean do not pass).
    fn passes(&self, ctx: &RowCtx) -> Result<bool, Error> {
        Ok(match &self.plan.where_expr {
            Some(expr) => eval(expr, ctx)? == Value::Bool(true),
            None => true,
        })
    }

    /// LIMIT reached (X1): the adapter stops pulling records — the old
    /// `Ok(None)` was indistinguishable from "filtered", so a limit-bodied
    /// query scrolled the whole object. Aggregate plans never consult it.
    pub fn limit_reached(&self) -> bool {
        !self.agg_state.is_some() && self.plan.limit.is_some_and(|n| self.emitted >= n)
    }

    /// Classify the projection list once (defense: the parse layer already
    /// guarantees every item is a bare aggregate call).
    fn classified(&mut self) -> Result<(), Error> {
        if self.agg_kinds.is_none() {
            let mut kinds = Vec::with_capacity(self.plan.projections.len());
            for item in &self.plan.projections {
                kinds.push(aggregate_kind(item)?);
            }
            self.agg_kinds = Some(kinds);
        }
        Ok(())
    }

    /// One record's projection: `Wild` → the record's fields verbatim
    /// (its own names as keys); `Item` → the evaluated expression under the
    /// alias, else the plain field name, else the expression text. JSON
    /// rows project the `Field` itself so a MISSING column serializes as
    /// `{}` (AWS: MISSING → empty record) rather than a present NULL; CSV
    /// keeps the eval-level collapse.
    fn project(&self, ctx: &RowCtx) -> Result<OutRow, Error> {
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for (i, item) in self.plan.projections.iter().enumerate() {
            match item {
                Projection::Wild => match ctx.record {
                    Record::Csv(cols) | Record::Parquet(cols) => {
                        keys.extend(cols.names.iter().cloned());
                        vals.extend(cols.fields.iter().cloned());
                    }
                    // JSON: the top-level keys in encounter order (objects);
                    // a scalar/array/MISSING row has no columns.
                    Record::Json(Some(serde_json::Value::Object(map))) => {
                        for (key, value) in map {
                            keys.push(key.clone());
                            vals.push(Field::Present(to_value(value)));
                        }
                    }
                    Record::Json(_) => {}
                },
                Projection::Item { expr, .. } => {
                    let field = match ctx.record {
                        Record::Json(_) => eval_field(expr, ctx)?,
                        _ => Field::Present(eval(expr, ctx)?),
                    };
                    vals.push(field);
                    keys.push(
                        self.item_keys[i]
                            .as_ref()
                            .expect("Item projection key precomputed")
                            .clone(),
                    );
                }
            }
        }
        Ok(OutRow { keys, vals })
    }
}

/// One passing record through every aggregate in the projection list.
fn accumulate(kinds: &[AggKind], state: &mut AggState, ctx: &RowCtx) -> Result<(), Error> {
    state.count += 1;
    for (kind, col) in kinds.iter().zip(state.per_col.iter_mut()) {
        match kind {
            AggKind::CountStar => {}
            AggKind::Count(e) => {
                if present_value(e, ctx)?.is_some() {
                    col.count += 1;
                }
            }
            AggKind::Sum(e) | AggKind::Avg(e) => {
                if let Some(v) = present_value(e, ctx)? {
                    let d = as_decimal(&v)?;
                    col.count += 1;
                    col.sum = Some(match col.sum {
                        None => d,
                        Some(s) => s.checked_add(d).ok_or_else(precision_overflow)?,
                    });
                }
            }
            AggKind::Min(e) => accumulate_extrema(e, true, col, ctx)?,
            AggKind::Max(e) => accumulate_extrema(e, false, col, ctx)?,
        }
    }
    Ok(())
}

/// The operand as a present non-NULL value (`eval_field` keeps MISSING);
/// every aggregate skips Missing and Null alike.
fn present_value(expr: &Expr, ctx: &RowCtx) -> Result<Option<Value>, Error> {
    Ok(match eval_field(expr, ctx)? {
        Field::Present(Value::Null) | Field::Missing => None,
        Field::Present(v) => Some(v),
    })
}

/// MIN/MAX accumulation, typed by the first contributing value (Task 7
/// ruling): a value that parses numerically makes the column numeric — the
/// extrema are parsed Decimals, so `min('10','2')` is 2, not "10", and a
/// later unparseable contributor cast-fails with a value error. An
/// unparseable first value (strings — the CSV case — reachable bool/JSON
/// shapes via their canonical text) makes it a strict-lexicographic string
/// column.
fn accumulate_extrema(
    expr: &Expr,
    is_min: bool,
    col: &mut AggCol,
    ctx: &RowCtx,
) -> Result<(), Error> {
    let Some(v) = present_value(expr, ctx)? else {
        return Ok(());
    };
    let current: &mut Option<Value> = if is_min { &mut col.min } else { &mut col.max };
    match col.extrema {
        Some(ExtremaMode::Numeric) => {
            let d = as_decimal(&v)?;
            let better = match current {
                None => true,
                Some(c) => {
                    let other = as_decimal(c)?;
                    if is_min { d < other } else { d > other }
                }
            };
            if better {
                *current = Some(Value::Decimal(d));
            }
        }
        Some(ExtremaMode::String) => {
            let text = display(&v);
            let better = match current {
                None => true,
                Some(c) => {
                    let current = display(c);
                    if is_min {
                        text < current
                    } else {
                        text > current
                    }
                }
            };
            if better {
                *current = Some(Value::String(text));
            }
        }
        None => match first_contrib_spine(&v)? {
            Some(d) => {
                col.extrema = Some(ExtremaMode::Numeric);
                *current = Some(Value::Decimal(d));
            }
            None => {
                col.extrema = Some(ExtremaMode::String);
                *current = Some(Value::String(display(&v)));
            }
        },
    }
    Ok(())
}

/// The first-contributor typing probe for MIN/MAX: a value that parses
/// numerically opens the numeric spine; a plain non-numeric string opens
/// the string spine. A number the spine cannot hold is a hard error — the
/// precision contract, never a silent string-column demotion (a 29-digit
/// "9…9" or an exponent form like `1e30` must not answer lexicographically
/// against "2"). One flow through `parse_number` (no duplicated digit
/// guard): its error is propagated for a numeric-shaped token and ignored
/// for plain text; `NaN`/`inf` are not finite as `f64` — not numeric-
/// shaped — so they stay a string column.
fn first_contrib_spine(v: &Value) -> Result<Option<Decimal>, Error> {
    match v {
        Value::Decimal(d) => Ok(Some(*d)),
        Value::Int(i) => Ok(Some(Decimal::from(*i))),
        Value::String(s) | Value::RawNumber(s) => match parse_number(s) {
            Ok(d) => Ok(Some(d)),
            // A numeric-shaped token the spine cannot hold — the precision
            // error surfaces now, never a demotion to string mode.
            Err(e) if numeric_shaped(s) => Err(e),
            // Plain text: string column, strict lexicographic.
            Err(_) => Ok(None),
        },
        _ => Ok(None),
    }
}

/// A numeric-shaped token: readable as `f64` and finite. Covers exponent
/// forms (`1e30` — few digits, so the digit guard never fires — that
/// exceed the 96-bit spine); `NaN`/`inf` parse as non-finite and stay
/// non-numeric.
fn numeric_shaped(s: &str) -> bool {
    s.parse::<f64>().is_ok_and(f64::is_finite)
}

/// One aggregate column's finished value: COUNT → Int; SUM/AVG/MIN/MAX with
/// zero contributing values → Null; AVG → Decimal scale 10.
fn finish_value(kind: &AggKind, state: &AggState, col: &AggCol) -> Result<Value, Error> {
    Ok(match kind {
        AggKind::CountStar => Value::Int(state.count as i64),
        AggKind::Count(_) => Value::Int(col.count as i64),
        AggKind::Sum(_) => match col.sum {
            Some(sum) => Value::Decimal(sum),
            None => Value::Null,
        },
        AggKind::Avg(_) => match (col.sum, col.count) {
            (Some(sum), n) if n > 0 => {
                let avg = sum
                    .checked_div(Decimal::from(n))
                    .ok_or_else(precision_overflow)?;
                Value::Decimal(avg.round_dp(10))
            }
            _ => Value::Null,
        },
        AggKind::Min(_) => col.min.clone().unwrap_or(Value::Null),
        AggKind::Max(_) => col.max.clone().unwrap_or(Value::Null),
    })
}

/// Classify one projection item as the aggregate it must be — the shape the
/// accumulation dispatches on; the parse layer rejects anything else, so
/// the two error arms below are defense-in-depth.
fn aggregate_kind(item: &Projection) -> Result<AggKind, Error> {
    let not_aggregate = || Error::Parse("non-aggregate expression in aggregate select list".into());
    let Projection::Item { expr, .. } = item else {
        return Err(not_aggregate());
    };
    let Expr::Function(f) = expr else {
        return Err(not_aggregate());
    };
    let Some(name) = single_part_name(&f.name) else {
        return Err(not_aggregate());
    };
    let FunctionArguments::List(list) = &f.args else {
        return Err(not_aggregate());
    };
    if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
        return Err(Error::Parse("distinct not supported".into()));
    }
    match (name.as_str(), list.args.as_slice()) {
        ("count", [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]) => Ok(AggKind::CountStar),
        (_, [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) if contains_aggregate(e) => {
            // Parity with the parse-layer rule: an aggregate nested in the
            // argument is not a bare call.
            Err(not_aggregate())
        }
        ("count", [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => {
            Ok(AggKind::Count(e.clone()))
        }
        ("sum", [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => Ok(AggKind::Sum(e.clone())),
        ("avg", [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => Ok(AggKind::Avg(e.clone())),
        ("min", [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => Ok(AggKind::Min(e.clone())),
        ("max", [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => Ok(AggKind::Max(e.clone())),
        _ => Err(not_aggregate()),
    }
}

/// A function name as one lowercase part; a qualified name is never one of
/// our aggregates.
fn single_part_name(name: &ObjectName) -> Option<String> {
    let [ObjectNamePart::Identifier(part)] = name.0.as_slice() else {
        return None;
    };
    Some(part.value.to_ascii_lowercase())
}

/// The shared 28-digit overflow error (same surface as arithmetic).
fn precision_overflow() -> Error {
    Error::Value("numeric value exceeds 28-digit precision".into())
}

/// The projection key of an expression without an alias: a path access is
/// named by its last named element (AWS: `s.projects[0].project_name` →
/// `project_name`), else the last identifier, else its text.
fn plain_key(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .expect("compound identifier is non-empty")
            .value
            .clone(),
        Expr::CompoundFieldAccess { root, access_chain } => access_key(root, access_chain),
        other => other.to_string(),
    }
}

/// The last named element of an access chain: walk backward for the last
/// dot-identifier or string subscript (the output column name AWS uses);
/// fall back to the root identifier, then to the expression text.
fn access_key(root: &Expr, chain: &[AccessExpr]) -> String {
    for step in chain.iter().rev() {
        match step {
            AccessExpr::Dot(Expr::Identifier(id)) => return id.value.clone(),
            AccessExpr::Subscript(Subscript::Index { index }) => {
                if let Some(SubKey::Key(k)) = subscript_key(index) {
                    return k;
                }
            }
            _ => {}
        }
    }
    match root {
        Expr::Identifier(id) => id.value.clone(),
        other => other.to_string(),
    }
}

/// One column reference → the record's field at that name. CSV: `_N` maps
/// positionally; header names lookup case-insensitive unless `quoted`;
/// duplicate names → `Ambiguous`; a USE-mode name that matches no header →
/// `MissingHeader`, headerless modes fall through to MISSING. JSON: the
/// attribute rules below. Parquet: the CSV spine minus MissingHeader (a
/// name matching no projected column is MISSING — see `parquet_field`).
fn column_value(rec: &Record, name: &str, style: NameStyle) -> Result<Option<Field>, Error> {
    match rec {
        Record::Csv(cols) => Ok(csv_field(&cols.fields, &cols.names, name, style)?),
        Record::Json(v) => Ok(Some(json_column(v.as_ref(), name, style)?)),
        Record::Parquet(cols) => parquet_field(&cols.fields, &cols.names, name, style),
    }
}

/// JSON column resolution: a present non-object value resolves any unquoted
/// name to itself (the scalar-row rule); an object looks the key up
/// case-insensitively (`quoted` → exact; folded duplicates → `Ambiguous`);
/// `_1` is the row itself unless the object carries a matching `_1` key
/// (field wins); absent → MISSING.
fn json_column(
    value: Option<&serde_json::Value>,
    name: &str,
    style: NameStyle,
) -> Result<Field, Error> {
    let Some(v) = value else {
        return Ok(Field::Missing);
    };
    match v {
        serde_json::Value::Object(map) => match json_lookup(map, name, style)? {
            None if name == "_1" => Ok(Field::Present(to_value(v))),
            Some(field) => Ok(Field::Present(to_value(field))),
            None => Ok(Field::Missing),
        },
        _ => Ok(Field::Present(to_value(v))),
    }
}

/// A JSON compound identifier path (`_1.dir_name`, `s.a.b`): the first part
/// resolves on the record (`_1`/the FROM alias → the row; any other name →
/// a field), the remaining parts walk object fields.
fn json_path(
    record: Option<&serde_json::Value>,
    parts: &[Ident],
    alias: &Option<String>,
) -> Result<Field, Error> {
    let (first, rest) = parts
        .split_first()
        .expect("compound identifier is non-empty");
    let mut field = json_root_field(record, &first.value, ident_style(first), alias)?;
    for part in rest {
        field = json_dot_step(field, &part.value, ident_style(part))?;
    }
    Ok(field)
}

/// The row itself under `_1` — the whole record value unless the object
/// carries a key matching `_1` (field wins).
fn json_row_ref(record: Option<&serde_json::Value>, style: NameStyle) -> Result<Field, Error> {
    let Some(v) = record else {
        return Ok(Field::Missing);
    };
    if let serde_json::Value::Object(map) = v
        && let Some(field) = json_lookup(map, "_1", style)?
    {
        return Ok(Field::Present(to_value(field)));
    }
    Ok(Field::Present(to_value(v)))
}

/// The row itself under the FROM alias — always the whole record value.
fn json_row_value(record: Option<&serde_json::Value>) -> Field {
    match record {
        None => Field::Missing,
        Some(v) => Field::Present(to_value(v)),
    }
}

/// The record-root resolution of a JSON identifier, shared by
/// `CompoundIdentifier` (`json_path`) and `CompoundFieldAccess`
/// (`json_access`): `_1` → the row (a matching field wins), the FROM alias →
/// the whole record value, any other name → a field.
fn json_root_field(
    record: Option<&serde_json::Value>,
    name: &str,
    style: NameStyle,
    alias: &Option<String>,
) -> Result<Field, Error> {
    match name {
        "_1" => json_row_ref(record, style),
        name if alias.as_deref() == Some(name) => Ok(json_row_value(record)),
        name => json_column(record, name, style),
    }
}

/// One dot step: an object key lookup (CI/exact per the part's quote);
/// anything else is MISSING.
fn json_dot_step(field: Field, name: &str, style: NameStyle) -> Result<Field, Error> {
    match field {
        Field::Missing => Ok(Field::Missing),
        Field::Present(Value::Json(j)) => json_dot(j, name, style),
        Field::Present(_) => Ok(Field::Missing),
    }
}

/// One dot look up on a boxed JSON value.
fn json_dot(j: Box<serde_json::Value>, name: &str, style: NameStyle) -> Result<Field, Error> {
    let serde_json::Value::Object(map) = j.as_ref() else {
        return Ok(Field::Missing);
    };
    match json_lookup(map, name, style)? {
        Some(v) => Ok(Field::Present(to_value(v))),
        None => Ok(Field::Missing),
    }
}

/// A JSON access chain (`s.projects[0].project_name`): the root expression
/// resolves on the record (`_1`/the FROM alias → the row; any other name →
/// a field), then each step is a dot or a bracket subscript; a MISSING
/// base stays MISSING and a non-match step is MISSING.
fn json_access(
    record: Option<&serde_json::Value>,
    root: &Expr,
    chain: &[AccessExpr],
    alias: &Option<String>,
) -> Result<Field, Error> {
    let mut field = match root {
        Expr::Identifier(id) => json_root_field(record, &id.value, ident_style(id), alias)?,
        // A non-identifier root has no path into the record.
        _ => Field::Missing,
    };
    for step in chain {
        field = json_step(field, step)?;
    }
    Ok(field)
}

/// One access-chain step.
fn json_step(field: Field, step: &AccessExpr) -> Result<Field, Error> {
    match (field, step) {
        (Field::Missing, _) => Ok(Field::Missing),
        (Field::Present(Value::Json(j)), AccessExpr::Dot(Expr::Identifier(id))) => {
            json_dot(j, &id.value, ident_style(id))
        }
        (Field::Present(Value::Json(_)), AccessExpr::Dot(_)) => Ok(Field::Missing),
        (Field::Present(Value::Json(j)), AccessExpr::Subscript(s)) => json_subscript(j, s),
        (Field::Present(_), _) => Ok(Field::Missing),
    }
}

/// One bracket subscript: a literal non-negative integer indexes an array
/// (out-of-range / not-an-array → MISSING); a literal string names an
/// object key exactly; anything else → MISSING.
fn json_subscript(j: Box<serde_json::Value>, s: &Subscript) -> Result<Field, Error> {
    let Subscript::Index { index } = s else {
        return Ok(Field::Missing);
    };
    match subscript_key(index) {
        Some(SubKey::Idx(i)) => match j.as_ref() {
            serde_json::Value::Array(elems) => Ok(elems
                .get(i)
                .map_or(Field::Missing, |e| Field::Present(to_value(e)))),
            _ => Ok(Field::Missing),
        },
        Some(SubKey::Key(k)) => match j.as_ref() {
            serde_json::Value::Object(map) => Ok(map
                .get(&k)
                .map_or(Field::Missing, |e| Field::Present(to_value(e)))),
            _ => Ok(Field::Missing),
        },
        None => Ok(Field::Missing),
    }
}

/// One subscript literal: number index or string key; other expressions
/// (negatives, column refs, arithmetic) are not a match.
fn subscript_key(index: &Expr) -> Option<SubKey> {
    match index {
        Expr::Value(v) => match &v.value {
            AstValue::Number(n, _) => n
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .map(|n| SubKey::Idx(n as usize)),
            AstValue::SingleQuotedString(s) => Some(SubKey::Key(s.clone())),
            _ => None,
        },
        // `s["k"]`: a double-quoted token parses as a quoted identifier —
        // the key's name is the identifier's value.
        Expr::Identifier(id) if id.quote_style.is_some() => Some(SubKey::Key(id.value.clone())),
        _ => None,
    }
}

/// The subscript literal's taxon: an array index or an object key name.
enum SubKey {
    Idx(usize),
    Key(String),
}

/// CSV column resolution, per the interface above.
fn csv_field(
    fields: &[Field],
    names: &[String],
    name: &str,
    style: NameStyle,
) -> Result<Option<Field>, Error> {
    // `_N` positional notation wins over headers.
    if let Some(index) = positional(name) {
        return Ok(Some(fields.get(index).cloned().unwrap_or(Field::Missing)));
    }
    // Header lookup: exact-case when quoted, case-insensitive otherwise.
    // A duplicate (case-folded for unquoted, literal for quoted) is
    // ambiguous — found by early-exit count, no per-lookup Vec (X8).
    let mut index: Option<usize> = None;
    for (i, header) in names.iter().enumerate() {
        let matches = if style.exact() {
            header == name
        } else {
            header.eq_ignore_ascii_case(name)
        };
        if matches {
            if index.is_some() {
                return Err(Error::Ambiguous(name.into()));
            }
            index = Some(i);
        }
    }
    match index {
        Some(i) => Ok(Some(fields.get(i).cloned().unwrap_or(Field::Missing))),
        None => {
            // USE-mode headers are the record's names; a named ref matching
            // none of them is a missing-header error. Headerless modes
            // (names are the positional `_1..` alias set) fall through to
            // MISSING.
            if is_positional_names(names) {
                Ok(Some(Field::Missing))
            } else {
                Err(Error::MissingHeader(name.into()))
            }
        }
    }
}

/// Parquet column resolution: the same spine as CSV (`_N` positional, CI
/// name lookup unquoted / exact quoted, duplicates ambiguous) except a name
/// matching no projected column is MISSING — never MissingHeader (that
/// code is CSV-header-specific; with projection pruning an unselected
/// schema column must look MISSING rather than error the stream).
fn parquet_field(
    fields: &[Field],
    names: &[String],
    name: &str,
    style: NameStyle,
) -> Result<Option<Field>, Error> {
    match csv_field(fields, names, name, style) {
        Ok(field) => Ok(field),
        Err(Error::MissingHeader(_)) => Ok(Some(Field::Missing)),
        Err(e) => Err(e),
    }
}

/// An identifier's spelling: sqlparser carries the quote bit on the ident.
fn ident_style(id: &Ident) -> NameStyle {
    if id.quote_style.is_some() {
        NameStyle::Quoted
    } else {
        NameStyle::Bare
    }
}

/// `_N` → zero-based index; `_0`/`_`/`_a` are not positional. Shared with
/// the parquet projection mask: an `_N` reference reads the full record
/// (the spine indexes the whole field list, never a pruned subset).
pub(crate) fn positional(name: &str) -> Option<usize> {
    let digits = name.strip_prefix('_')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = digits.parse::<usize>().ok()?;
    (n >= 1).then(|| n - 1)
}

/// The headerless alias set `_1.._n` (NONE/IGNORE rows carry it; USE rows
/// carry the header names) — the only signal the record offers for whether
/// a plain named ref may fall through to MISSING.
fn is_positional_names(names: &[String]) -> bool {
    // No `format!` per name (X8): strip `_`, parse the digits.
    names.iter().enumerate().all(|(i, n)| {
        let Some(digits) = n.strip_prefix('_') else {
            return false;
        };
        !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
            && digits.parse::<usize>().ok() == Some(i + 1)
    })
}

/// Missing-aware resolution of an operand: column references go through
/// `column_value` (an absent field stays `Field::Missing`); any other
/// expression produces a value and is never MISSING.
fn eval_field(expr: &Expr, ctx: &RowCtx) -> Result<Field, Error> {
    match expr {
        Expr::Identifier(id) => column_field(ctx, &id.value, ident_style(id)),
        Expr::CompoundIdentifier(parts) => match ctx.record {
            Record::Json(v) => json_path(v.as_ref(), parts, ctx.alias),
            _ => {
                let last = parts.last().expect("compound identifier is non-empty");
                column_field(ctx, &last.value, ident_style(last))
            }
        },
        // Path access on flat CSV is not meaningful — the full text never
        // matches a header → MISSING; JSON resolves the real access chain.
        Expr::CompoundFieldAccess { root, access_chain } => match ctx.record {
            Record::Json(v) => json_access(v.as_ref(), root, access_chain, ctx.alias),
            _ => column_field(ctx, &expr.to_string(), NameStyle::Bare),
        },
        Expr::JsonAccess { .. } => column_field(ctx, &expr.to_string(), NameStyle::Bare),
        // `(x) IS NULL` ≡ `x IS NULL`: parenthesized columns stay MISSING
        // rather than collapsing through `eval` into a present NULL.
        Expr::Nested(e) => eval_field(e, ctx),
        other => Ok(Field::Present(eval(other, ctx)?)),
    }
}

fn column_field(ctx: &RowCtx, name: &str, style: NameStyle) -> Result<Field, Error> {
    Ok(column_value(ctx.record, name, style)?.unwrap_or(Field::Missing))
}

/// The single expression argument the `IS [NOT] MISSING` hook emits.
fn sentinel_operand(f: &Function) -> Result<Expr, Error> {
    match &f.args {
        FunctionArguments::List(list) => match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => Ok(e.clone()),
            _ => Err(Error::Value("internal: unexpected aggregate call".into())),
        },
        _ => Err(Error::Value("internal: unexpected aggregate call".into())),
    }
}

/// The MISSING sentinel is a single-part UNQUOTED identifier — the dialect
/// hook always inserts it so. A qualified or quoted call must not adopt
/// MISSING semantics (the engine's own guard; the R15 parse scan is gone —
/// the per-request uuid name is unguessable, so user text cannot collide).
/// The dialect's chained-form check (`is_sentinel_expr`) calls this too:
/// one definition of sentinel identity across both sides.
pub(crate) fn is_sentinel_call(name: &ObjectName, sentinel: &str) -> bool {
    let [ObjectNamePart::Identifier(part)] = name.0.as_slice() else {
        return false;
    };
    part.quote_style.is_none() && part.value.eq_ignore_ascii_case(sentinel)
}

/// Why the evaluator cannot run an expression form. The single definition of
/// what the engine supports: `eval`'s catch-all arms and the validator's
/// parse-time walk both consult [`unsupported_form`], so the request-level
/// refusal and the runtime error cannot drift apart.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub(crate) enum UnsupportedForm {
    #[display("unsupported expression: {0}")]
    Expression(Cow<'static, str>),
    #[display("unsupported function: {0}")]
    Function(String),
    /// A literal kind the converter has no mapping for (`X'…'`, `N'…'`, `$$…$$`).
    /// Carries the evaluator's own message verbatim: a second prefix here
    /// would be a diagnostic nobody else spells.
    #[display("{0}")]
    Literal(String),
    /// A statically decidable operand defect (`1e999`, a bad `ESCAPE`).
    /// Same verbatim-text rule as [`Self::Literal`].
    #[display("{0}")]
    Malformed(String),
}

impl From<UnsupportedForm> for Error {
    /// The verdict's channel — the one place it is decided, so the parse-time
    /// walk and `eval` cannot disagree about which verdict answers `Parse` and
    /// which answers `Unsupported`.
    ///
    /// A malformed operand (`ESCAPE 'ab'`, `1e999`) is a malformed *query*:
    /// `Parse`, the "LIMIT must be a positive integer" precedent. Every other
    /// verdict is a declined form and carries the `Display` text verbatim —
    /// including `Literal`, whose text is the evaluator's own message.
    fn from(form: UnsupportedForm) -> Self {
        match form {
            UnsupportedForm::Malformed(m) => Error::Parse(m),
            other => Error::Unsupported(other.to_string()),
        }
    }
}

/// The longest rendered form a diagnostic may carry. The echo is otherwise
/// bounded only by `MAX_EXPRESSION` (256 KiB), which would put a same-size
/// string in the 400 body; 256 chars is past any diagnostic's useful length.
const RENDERED_DIAGNOSTIC: usize = 256;

/// `text` cut to [`RENDERED_DIAGNOSTIC`] chars on a char boundary.
///
/// One scan and no copy: `char_indices().nth(n)` yields the byte offset of the
/// first char past the limit — or `None` when there is nothing to cut — so
/// neither the length check nor the truncation allocates.
fn bounded(mut text: String) -> String {
    match text.char_indices().nth(RENDERED_DIAGNOSTIC) {
        None => text,
        Some((cut, _)) => {
            text.truncate(cut);
            text.push_str("...");
            text
        }
    }
}

/// The rendered SQL text of a form with no dedicated name — with the request's
/// MISSING sentinel name replaced by the operator it stands for, and bounded.
///
/// The sentinel is the evaluator's own production (`__s3_is_missing_<uuid>`,
/// minted per request), and a rendered subtree can contain it: the dialect's
/// `parse_infix` hook rewrites `X IS MISSING` wherever it appears, including
/// inside a form this predicate rejects. Echoing the text verbatim would hand
/// the client an internal symbol. Nothing is exploitable today (the uuid is
/// per-request and that request has already been parsed), but the name is not
/// the client's business; `IS MISSING` is what the user wrote.
fn rendered(expr: &Expr, missing_name: &str) -> String {
    let text = expr.to_string();
    // `replace` copies the whole string, and the sentinel is absent from
    // almost every diagnostic — probe before paying for the copy.
    if missing_name.is_empty() || !text.contains(missing_name) {
        return bounded(text);
    }
    bounded(text.replace(missing_name, "IS MISSING"))
}

/// The function allowlist the predicate consults (design Q8): exactly the
/// aggregate set in this spec. The scalar-function library spec appends
/// scalar names HERE, never to `is_aggregate_name` — that predicate also
/// decides aggregate mode (governing `*` and the scan-accumulate path), so
/// widening it would make `SELECT LOWER(s.a) …` take the wrong execution
/// path.
fn is_allowed_function(name: &ObjectName) -> bool {
    is_aggregate_name(name)
}

/// Classify one node. `None` means the engine evaluates this form; `Some`
/// names the reason it cannot. Switches on the `Expr` *variant* only — an
/// identifier whose text happens to be a keyword (`s.CAST`) is supported,
/// which is the recorded AWS divergence (spec Q2/Q8).
///
/// The payload-carrying forms (`Value`, `Like`) are judged by the evaluator's
/// own operand checks below rather than by a second implementation of them, so
/// a literal or an `ESCAPE` cannot pass the walk and then fail per row: the
/// walk runs the same `literal`/`like_escape` the evaluator does and keeps
/// their message, only on a request-level channel.
pub(crate) fn unsupported_form(expr: &Expr, missing_name: &str) -> Option<UnsupportedForm> {
    // Forms `eval` implements whose *shape* is the whole story, taken from its
    // positive match arms.
    let supported = matches!(
        expr,
        Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::CompoundFieldAccess { .. }
            | Expr::JsonAccess { .. }
            | Expr::Nested(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
    );
    if supported {
        return None;
    }
    let named = |name: &'static str| Some(UnsupportedForm::Expression(Cow::Borrowed(name)));
    // An unnamed form keeps its rendered text, bounded.
    let owned = |text: String| Some(UnsupportedForm::Expression(Cow::Owned(bounded(text))));
    match expr {
        // The converter is the only judge of a literal, so this arm runs it and
        // keeps its verdict: a kind with no mapping (`X'…'`) stays the
        // `Unsupported` it always was, while a value outside the decimal model
        // (`1e999`, a 29-digit literal) becomes a request-level parse error
        // instead of a per-row one. The conversion result is dropped — eval
        // re-derives it per row, which is what it did before this arm existed.
        Expr::Value(v) => match literal(&v.value) {
            Ok(_) => None,
            Err(Error::Unsupported(m)) => Some(UnsupportedForm::Literal(bounded(m))),
            Err(Error::Value(m)) => Some(UnsupportedForm::Malformed(bounded(m))),
            // `literal` has no other channel; kept so a future one cannot
            // silently fall through to `None`.
            Err(other) => Some(UnsupportedForm::Malformed(bounded(other.to_string()))),
        },
        // An `ESCAPE` operand, same reasoning: the pattern shape is supported,
        // the operand goes through the evaluator's own single-character check.
        Expr::Like {
            any: false,
            escape_char,
            ..
        } => match like_escape(escape_char) {
            Ok(_) => None,
            Err(Error::Value(m)) => Some(UnsupportedForm::Malformed(bounded(m))),
            Err(other) => Some(UnsupportedForm::Malformed(bounded(other.to_string()))),
        },
        Expr::UnaryOp { op, .. } => match op {
            UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus => None,
            other => owned(other.to_string()),
        },
        Expr::BinaryOp { op, .. } => match op {
            BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => None,
            other => owned(other.to_string()),
        },
        // The per-request sentinel is the evaluator's own production; the
        // allowlist's names are evaluated by the accumulate path, not `eval`.
        Expr::Function(f) => {
            if is_sentinel_call(&f.name, missing_name) || is_allowed_function(&f.name) {
                None
            } else {
                // Named as written: the diagnostic preserves the user's
                // spelling (`unsupported function: LOWER`), so this is a
                // case- and quote-preserving single-part lookup. Reusing
                // `single_part_name` here would lowercase the name — that
                // lowercasing is its aggregate-matching job, not a display
                // rule.
                Some(UnsupportedForm::Function(bounded(
                    match f.name.0.as_slice() {
                        [ObjectNamePart::Identifier(part)] => part.value.clone(),
                        _ => f.name.to_string(),
                    },
                )))
            }
        }
        // LIKE ANY is named here so both layers report the same thing:
        // the parse-time walk consults this arm, and `eval`'s dedicated
        // `any: true` guard routes through the predicate too.
        Expr::Like { any: true, .. } => named("LIKE ANY"),
        Expr::ILike { .. } => named("ILIKE"),
        Expr::Case { .. } => named("CASE"),
        // `TRY_CAST`/`SAFE_CAST` are not their own variants in 0.62 — they
        // are `CastKind`s. `CastKind` has exactly these four variants and no
        // `Display` impl, so match exhaustively on it.
        Expr::Cast { kind, .. } => named(match kind {
            CastKind::Cast | CastKind::DoubleColon => "CAST",
            CastKind::TryCast => "TRY_CAST",
            CastKind::SafeCast => "SAFE_CAST",
        }),
        Expr::Substring { .. } => named("SUBSTRING"),
        Expr::Trim { .. } => named("TRIM"),
        Expr::Extract { .. } => named("EXTRACT"),
        Expr::Ceil { .. } => named("CEIL"),
        Expr::Floor { .. } => named("FLOOR"),
        Expr::Position { .. } => named("POSITION"),
        Expr::Interval(_) => named("INTERVAL"),
        Expr::TypedString { .. } => named("TYPED STRING"),
        Expr::IsDistinctFrom(..) | Expr::IsNotDistinctFrom(..) => named("IS DISTINCT FROM"),
        // No subquery arm: `reject_subqueries` (`sql.rs`) refuses `Subquery`,
        // `InSubquery` and `Exists` on every expression *before* this predicate
        // is consulted, so naming them here would be a second authority for one
        // decision — and a second message (`reject_subqueries` answers
        // `unsupported: subquery`). Unreachable from either layer: the walk runs
        // behind it, and `eval` only ever sees a plan `parse` built. The
        // catch-all below still refuses the shapes, rendering them.
        // Anything else keeps the text it produced before this predicate
        // existed, only bounded and with the sentinel name scrubbed: the
        // pre-predicate catch-all was `Error::Unsupported(<rendered>)`, so an
        // unnamed variant gains the `expression:` prefix (named deliberately
        // by the spec) and nothing else.
        other => Some(UnsupportedForm::Expression(Cow::Owned(rendered(
            other,
            missing_name,
        )))),
    }
}

/// Evaluate a parsed expression tree to a scalar. `eval` runs no code of
/// its own — the `Expr` is sqlparser's AST and every arithmetic/comparison
/// rule below is data-driven; a missing column collapses to `Value::Null`
/// here (`eval_field` is the MISSING-aware path).
fn eval(expr: &Expr, ctx: &RowCtx) -> Result<Value, Error> {
    match expr {
        Expr::Value(v) => literal(&v.value),
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::CompoundFieldAccess { .. }
        | Expr::JsonAccess { .. } => match eval_field(expr, ctx)? {
            Field::Present(v) => Ok(v),
            Field::Missing => Ok(Value::Null),
        },
        Expr::Nested(e) => eval(e, ctx),
        // The `un_op @` binding is load-bearing: the destructured `expr`
        // field shadows the whole node, and the predicate must classify the
        // UnaryOp itself, not its operand.
        un_op @ Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let v = eval(expr, ctx)?;
                Ok(match v {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    _ => Value::Bool(false),
                })
            }
            UnaryOperator::Minus => unary_minus(eval(expr, ctx)?),
            // `+` is the identity in our arithmetic.
            UnaryOperator::Plus => eval(expr, ctx),
            // The whole node, not the operator: the predicate's own arm names
            // the operator.
            _ => Err(unsupported(un_op, ctx)),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = eval(left, ctx)?;
            // Short-circuit (review 2026-09-06b R11): `false AND x` and
            // `true OR x` are known without examining x — the dead arm's
            // errors (division by zero, cast failure) must not fire.
            match op {
                BinaryOperator::And if matches!(l, Value::Bool(false)) => {
                    return Ok(Value::Bool(false));
                }
                BinaryOperator::Or if matches!(l, Value::Bool(true)) => {
                    return Ok(Value::Bool(true));
                }
                _ => {}
            }
            let r = eval(right, ctx)?;
            match op {
                BinaryOperator::And => Ok(value_and(l, r)),
                BinaryOperator::Or => Ok(value_or(l, r)),
                BinaryOperator::Eq => Ok(Value::Bool(equal(&l, &r))),
                BinaryOperator::NotEq => Ok(Value::Bool(not_equal(&l, &r))),
                BinaryOperator::Lt => Ok(Value::Bool(compare(&l, &r) == Some(Ordering::Less))),
                BinaryOperator::LtEq => Ok(Value::Bool(compare(&l, &r).is_some_and(|o| o.is_le()))),
                BinaryOperator::Gt => Ok(Value::Bool(compare(&l, &r) == Some(Ordering::Greater))),
                BinaryOperator::GtEq => Ok(Value::Bool(compare(&l, &r).is_some_and(|o| o.is_ge()))),
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => arith(op, l, r),
                _ => Err(unsupported(expr, ctx)),
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in(expr, list, *negated, ctx),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => eval_between(expr, low, high, *negated, ctx),
        // The IsNot* forms are the pure negations of their cousins (R12):
        // one hit-predicate per pair, flipped by the negated arm.
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            let hit = matches!(eval_field(e, ctx)?, Field::Present(Value::Null));
            Ok(Value::Bool(hit != matches!(expr, Expr::IsNotNull(_))))
        }
        Expr::IsTrue(e) | Expr::IsNotTrue(e) => {
            let hit = matches!(eval_field(e, ctx)?, Field::Present(Value::Bool(true)));
            Ok(Value::Bool(hit != matches!(expr, Expr::IsNotTrue(_))))
        }
        Expr::IsFalse(e) | Expr::IsNotFalse(e) => {
            let hit = matches!(eval_field(e, ctx)?, Field::Present(Value::Bool(false)));
            Ok(Value::Bool(hit != matches!(expr, Expr::IsNotFalse(_))))
        }
        Expr::Function(f) => {
            // The dialect's parse_infix hook (sqlparser 0.62 has no
            // IsMissing variant) is the only function-form producer besides
            // the aggregates; the per-request uuid name is what it emitted
            // (the name is carried on the ctx — derived once per request).
            if is_sentinel_call(&f.name, ctx.missing_name) {
                let operand = sentinel_operand(f)?;
                return Ok(Value::Bool(matches!(
                    eval_field(&operand, ctx)?,
                    Field::Missing
                )));
            }
            match unsupported_form(expr, ctx.missing_name) {
                Some(form) => Err(form.into()),
                // The predicate calls this supported — the drift case (an
                // aggregate name reaching `eval`). Keep the internal error it
                // had; the corpus is what catches this.
                None => Err(Error::Value("internal: unexpected aggregate call".into())),
            }
        }
        // Snowflake's `LIKE ANY` is outside the AWS surface and the covered
        // grammar: refuse rather than silently run plain LIKE semantics. It
        // goes through the predicate like everything else, so parse time and
        // runtime name the form identically and no short name is hard-coded
        // here.
        Expr::Like { any: true, .. } => Err(unsupported(expr, ctx)),
        Expr::Like {
            negated,
            any: false,
            expr,
            pattern,
            escape_char,
        } => eval_like(expr, pattern, escape_char, *negated, ctx),
        // `ILike` keeps no arm of its own: the catch-all names it through the
        // predicate. `LIKE ANY` cannot reach `eval_like` — this arm precedes
        // it, and the predicate names the form in the catch-all behind both —
        // so `eval_like` carries no `any` backstop to fall out of step.
        other => Err(unsupported(other, ctx)),
    }
}

/// Runtime diagnostic for a form the predicate rejects. The predicate is the
/// single authority; the fallback below is unreachable by construction and
/// exists only so a future drift cannot silently answer `Ok`.
fn unsupported(expr: &Expr, ctx: &RowCtx) -> Error {
    match unsupported_form(expr, ctx.missing_name) {
        // Same channel mapping the walk uses — one home for it, so the two
        // layers cannot disagree about which verdict is `Parse`.
        Some(form) => form.into(),
        None => Error::Unsupported(rendered(expr, ctx.missing_name)),
    }
}

/// SQL literals: numbers → `Int` when they fit, else `Decimal`.
fn literal(v: &AstValue) -> Result<Value, Error> {
    match v {
        AstValue::Number(n, _) => match n.parse::<i64>() {
            Ok(i) => Ok(Value::Int(i)),
            Err(_) => parse_number(n).map(Value::Decimal),
        },
        AstValue::SingleQuotedString(s) => Ok(Value::String(s.clone())),
        AstValue::Boolean(b) => Ok(Value::Bool(*b)),
        AstValue::Null => Ok(Value::Null),
        other => Err(Error::Unsupported(other.to_string())),
    }
}

/// Unary minus: numeric negation with Int overflow promoted to Decimal.
fn unary_minus(v: Value) -> Result<Value, Error> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Int(i) => match i.checked_neg() {
            Some(n) => Ok(Value::Int(n)),
            None => Ok(Value::Decimal(-Decimal::from(i))),
        },
        Value::Decimal(d) => Ok(Value::Decimal(-d)),
        Value::String(s) | Value::RawNumber(s) => parse_number(&s).map(|d| Value::Decimal(-d)),
        Value::Bool(_) | Value::Json(_) => Err(not_numeric(&v)),
    }
}

/// SQL three-valued `AND`: false dominates, null propagates, non-boolean
/// operands are FALSE (the "string in boolean position" rule).
fn value_and(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
        (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Bool(false),
    }
}

/// SQL three-valued `OR`: true dominates, null propagates, non-boolean
/// operands are FALSE.
fn value_or(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
        (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Bool(false),
    }
}

/// `IN` with the `negated` flag; a Null subject or element yields an
/// UNKNOWN that negated forms do not flip to TRUE.
fn eval_in(expr: &Expr, list: &[Expr], negated: bool, ctx: &RowCtx) -> Result<Value, Error> {
    let subject = eval(expr, ctx)?;
    let mut found = false;
    let mut unknown = matches!(subject, Value::Null);
    for item in list {
        let v = eval(item, ctx)?;
        unknown |= matches!(v, Value::Null);
        if equal(&subject, &v) {
            found = true;
            break;
        }
    }
    let base = match (found, unknown) {
        (true, _) => Value::Bool(true),
        (false, true) => Value::Null,
        (false, false) => Value::Bool(false),
    };
    Ok(if negated { invert_bool(base) } else { base })
}

/// `BETWEEN` with the `negated` flag; incomparable operands (Null, or a
/// non-numeric string vs numbers) are UNKNOWN.
fn eval_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    ctx: &RowCtx,
) -> Result<Value, Error> {
    let subject = eval(expr, ctx)?;
    let lo = eval(low, ctx)?;
    let hi = eval(high, ctx)?;
    let value = match (compare(&subject, &lo), compare(&subject, &hi)) {
        (Some(a), Some(b)) => Value::Bool(a.is_ge() && b.is_le()),
        _ => Value::Null,
    };
    Ok(if negated { invert_bool(value) } else { value })
}

fn invert_bool(v: Value) -> Value {
    match v {
        Value::Bool(b) => Value::Bool(!b),
        other => other,
    }
}

/// One pattern element after ESCAPE resolution.
enum LikeTok {
    Star,
    Single,
    Char(char),
}

/// `LIKE` per the plan: case-sensitive, `%` any sequence (incl. empty), `_`
/// exactly one char, optional one-char `ESCAPE`; the `negated` flag flips
/// the result. Only String operands match — Null/MISSING, numbers, bools
/// and JSON are false — with the match text taken from `display` (the row
/// model's canonical text form; for strings it is the string itself).
fn eval_like(
    expr: &Expr,
    pattern: &Expr,
    escape_char: &Option<ValueWithSpan>,
    negated: bool,
    ctx: &RowCtx,
) -> Result<Value, Error> {
    let escape = like_escape(escape_char)?;
    let subject = eval(expr, ctx)?;
    let pattern = eval(pattern, ctx)?;
    let matched = match (&subject, &pattern) {
        (Value::String(_), Value::String(_)) => {
            Some(like_match(&display(&subject), &display(&pattern), escape)?)
        }
        _ => None,
    };
    // Non-string operands are FALSE for the whole LIKE family — a negated
    // form must not flip the mismatch to true (review 2026-09-06b R13;
    // plan: "non-string operands → false").
    Ok(Value::Bool(match matched {
        Some(m) if negated => !m,
        Some(m) => m,
        None => false,
    }))
}

/// The ESCAPE operand: exactly one character. sqlparser 0.62 carries it as
/// a `ValueWithSpan` (a single-quoted literal), validated here — the
/// parser accepts any literal string, so a bad one is a value error.
fn like_escape(v: &Option<ValueWithSpan>) -> Result<Option<char>, Error> {
    match v {
        None => Ok(None),
        Some(w) => {
            let ast::Value::SingleQuotedString(s) = &w.value else {
                return Err(Error::Value("ESCAPE must be a single character".into()));
            };
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(Some(c)),
                _ => Err(Error::Value("ESCAPE must be a single character".into())),
            }
        }
    }
}

/// The DP-cell ceiling of one LIKE pair (review 2026-09-06b R7): 256 KiB
/// pattern × 1 MB record ≈ 2.6×10⁸ cells would pin a spawn_blocking worker
/// for seconds-to-minutes on a hostile query — a pair past the cap is
/// refused with a value error, not run. 10⁷ cells is high-end single-digit
/// milliseconds.
const LIKE_CELL_CAP: u64 = 10_000_000;

// The per-scan LIKE memo (X7): a literal pattern is constant across
// records — re-deriving its tokens and re-allocating the two DP rows per
// record was pure waste. One slot (the last pattern) covers the common
// case; a per-row pattern simply misses and rebuilds. The memo is a pure
// data cache — `like_match` runs no nested eval under its borrow, so
// re-entrancy is impossible (the engine is sync, one thread per stream).
thread_local! {
    static LIKE_MEMO: RefCell<LikeMemo> =
        const { RefCell::new(LikeMemo::new()) };
}

#[derive(Default)]
struct LikeMemo {
    pattern: String,
    escape: Option<char>,
    tokens: Vec<LikeTok>,
    prev: Vec<bool>,
    cur: Vec<bool>,
}

impl LikeMemo {
    const fn new() -> Self {
        Self {
            pattern: String::new(),
            escape: None,
            tokens: Vec::new(),
            prev: Vec::new(),
            cur: Vec::new(),
        }
    }
}

/// O(n·m) wildcard DP — no backtracking, so `%`-heavy patterns stay
/// polynomial under the expression and record caps (review 2026-09-05 #5);
/// the cell count (pattern tokens × text chars) is capped (R7).
fn like_match(text: &str, pattern: &str, escape: Option<char>) -> Result<bool, Error> {
    let text: Vec<char> = text.chars().collect();
    LIKE_MEMO.with(|slot| {
        let mut memo = slot.borrow_mut();
        if memo.pattern != pattern || memo.escape != escape {
            memo.pattern = pattern.to_string();
            memo.escape = escape;
            memo.tokens = like_tokens(pattern, escape);
        }
        let m = text.len();
        if memo.tokens.len() as u64 * (m as u64 + 1) > LIKE_CELL_CAP {
            return Err(Error::Value("LIKE pattern too large".into()));
        }
        let LikeMemo {
            tokens, prev, cur, ..
        } = &mut *memo;
        prev.clear();
        prev.resize(m + 1, false);
        cur.clear();
        cur.resize(m + 1, false);
        prev[0] = true;
        for tok in tokens.iter() {
            // `%` may match the empty prefix; nothing else may.
            cur[0] = matches!(tok, LikeTok::Star) && prev[0];
            for j in 1..=m {
                cur[j] = match tok {
                    LikeTok::Star => prev[j] || cur[j - 1],
                    LikeTok::Single => prev[j - 1],
                    LikeTok::Char(c) => prev[j - 1] && text[j - 1] == *c,
                };
            }
            mem::swap(prev, cur);
        }
        Ok(prev[m])
    })
}

/// ESCAPE resolution to tokens: the escape char literalizes the next
/// pattern char (a trailing escape stays literal); consecutive `%` runs
/// collapse to one star.
fn like_tokens(pattern: &str, escape: Option<char>) -> Vec<LikeTok> {
    let p: Vec<char> = pattern.chars().collect();
    let mut toks = Vec::with_capacity(p.len());
    let mut i = 0;
    while i < p.len() {
        if escape == Some(p[i]) {
            let literal = p.get(i + 1).copied().unwrap_or(p[i]);
            toks.push(LikeTok::Char(literal));
            i += 2;
        } else {
            match p[i] {
                '%' if !matches!(toks.last(), Some(LikeTok::Star)) => toks.push(LikeTok::Star),
                '%' => {}
                '_' => toks.push(LikeTok::Single),
                c => toks.push(LikeTok::Char(c)),
            }
            i += 1;
        }
    }
    toks
}

/// Equality per the comparison rules: Bool only with Bool, everything else
/// numeric-coerced or string-lexicographic; Null/incomparable → false.
fn equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => compare(a, b).is_some_and(|o| o.is_eq()),
    }
}

/// `!=` keeps the incomparable-false rule (Null and parse failures compare
/// false even negated).
fn not_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x != y,
        _ => compare(a, b).is_some_and(|o| !o.is_eq()),
    }
}

/// Total order between comparable values, `None` when incomparable
/// (Null/bool/JSON operands, string-vs-numeric parse failure).
fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Bool(_), _) | (_, Value::Bool(_)) => None,
        (Value::Json(_), _) | (_, Value::Json(_)) => None,
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => match (numeric(a), numeric(b)) {
            (Some(x), Some(y)) => Some(x.cmp(&y)),
            _ => None,
        },
    }
}

/// The numeric spine of a value for comparison: numbers direct, strings
/// and raw JSON numbers parsed lazily — a parse failure makes the pair
/// incomparable (comparison yields false, never an error).
fn numeric(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some(*d),
        Value::Int(i) => Some(Decimal::from(*i)),
        Value::String(s) | Value::RawNumber(s) => parse_number(s).ok(),
        _ => None,
    }
}

/// Arithmetic per the plan: Int×Int stays Int with checked overflow
/// promoted to Decimal; `/` is Decimal scale 10; `%` integral-only;
/// division by zero is a value error.
fn arith(op: &BinaryOperator, a: Value, b: Value) -> Result<Value, Error> {
    // Null propagates through arithmetic.
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    if *op == BinaryOperator::Modulo {
        return modulo(&a, &b);
    }
    if let (Value::Int(x), Value::Int(y)) = (&a, &b) {
        let fast = match op {
            BinaryOperator::Plus => x.checked_add(*y).map(Value::Int),
            BinaryOperator::Minus => x.checked_sub(*y).map(Value::Int),
            BinaryOperator::Multiply => x.checked_mul(*y).map(Value::Int),
            _ => None,
        };
        if let Some(v) = fast {
            return Ok(v);
        }
        // overflow → promote to Decimal
        return decimal_arith(op, Decimal::from(*x), Decimal::from(*y));
    }
    decimal_arith(op, as_decimal(&a)?, as_decimal(&b)?)
}

/// Strict numeric coercion for arithmetic: numbers direct, strings and
/// raw numbers parsed (a failure is an error here — only comparisons
/// tolerate it); booleans and JSON are not numbers.
fn as_decimal(v: &Value) -> Result<Decimal, Error> {
    match v {
        Value::Decimal(d) => Ok(*d),
        Value::Int(i) => Ok(Decimal::from(*i)),
        Value::String(s) | Value::RawNumber(s) => parse_number(s),
        // Null propagates before arithmetic reaches here.
        Value::Null => Ok(Decimal::ZERO),
        Value::Bool(_) | Value::Json(_) => Err(not_numeric(v)),
    }
}

fn not_numeric(v: &Value) -> Error {
    Error::Value(format!("invalid numeric value: {}", display(v)))
}

/// Decimal arithmetic: strings/raw numbers parse here (strict — only
/// comparisons are tolerant of parse failure).
fn decimal_arith(op: &BinaryOperator, x: Decimal, y: Decimal) -> Result<Value, Error> {
    match op {
        BinaryOperator::Plus => x
            .checked_add(y)
            .map(Value::Decimal)
            .ok_or_else(precision_overflow),
        BinaryOperator::Minus => x
            .checked_sub(y)
            .map(Value::Decimal)
            .ok_or_else(precision_overflow),
        BinaryOperator::Multiply => x
            .checked_mul(y)
            .map(Value::Decimal)
            .ok_or_else(precision_overflow),
        BinaryOperator::Divide => {
            if y.is_zero() {
                return Err(Error::Value("division by zero".into()));
            }
            x.checked_div(y)
                .map(|d| Value::Decimal(d.round_dp(10)))
                .ok_or_else(precision_overflow)
        }
        BinaryOperator::Modulo => modulo(&Value::Decimal(x), &Value::Decimal(y)),
        other => Err(Error::Unsupported(other.to_string())),
    }
}

/// `%` is integral-only: operands must be whole numbers; the result is
/// Int (checked; the MIN % -1 corner promotes to a Decimal remainder).
fn modulo(a: &Value, b: &Value) -> Result<Value, Error> {
    let x = integral(a).ok_or_else(|| not_numeric(a))?;
    let y = integral(b).ok_or_else(|| not_numeric(b))?;
    if y == 0 {
        return Err(Error::Value("division by zero".into()));
    }
    Ok(match x.checked_rem(y) {
        Some(r) => Value::Int(r),
        None => Value::Decimal(Decimal::from(x) % Decimal::from(y)),
    })
}

/// A whole number: Int directly, Decimal/string/raw numbers once the
/// fractional part is zero (the `ToPrimitive::to_i64` truncates, so the
/// integrality check must come first).
fn integral(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Decimal(d) => (d.fract().is_zero()).then(|| d.to_i64()).flatten(),
        Value::String(s) | Value::RawNumber(s) => {
            let d = parse_number(s).ok()?;
            (d.fract().is_zero()).then(|| d.to_i64()).flatten()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    // `AttachedToken` is not re-exported at `sqlparser::ast` (its only name
    // there is a private `use`), so it is imported by its public path.
    use sqlparser::ast::helpers::attached_token::AttachedToken;
    use sqlparser::ast::{
        CaseWhen, CeilFloorKind, DataType, DateTimeField, ExtractSyntax, FunctionArgumentList,
        Interval, TypedString,
    };

    use super::*;
    use crate::{
        row::{Columns, Value},
        sql::{FromClause, SentinelNames, parse},
    };

    fn csv(fields: &[&str], names: &[&str]) -> Record {
        Record::Csv(Columns::from_names(
            fields
                .iter()
                .map(|f| Field::Present(Value::String(f.to_string())))
                .collect(),
            names.iter().map(|n| n.to_string()).collect(),
        ))
    }

    fn s(v: &str) -> Field {
        Field::Present(Value::String(v.to_string()))
    }

    /// Run `sql` over `records`, panicking on an engine error.
    #[track_caller]
    fn run(sql: &str, records: Vec<Record>) -> Vec<OutRow> {
        let mut engine = Engine::new(parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}")));
        let mut rows = Vec::new();
        for rec in records {
            if let Some(row) = engine.next(rec).unwrap_or_else(|e| panic!("{sql}: {e}")) {
                rows.push(row);
            }
        }
        rows
    }

    /// Run `sql` over `records`, then `finish` — the aggregate row (Task 7).
    #[track_caller]
    fn run_agg(sql: &str, records: Vec<Record>) -> OutRow {
        let mut engine = Engine::new(parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}")));
        for rec in records {
            engine.next(rec).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        engine
            .finish()
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .unwrap_or_else(|| panic!("{sql}: expected an aggregate row"))
    }

    #[test]
    fn where_limit_filters_then_caps() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._3 > 100 LIMIT 2",
            vec![
                csv(&["a", "b", "150"], &["_1", "_2", "_3"]),
                csv(&["c", "d", "50"], &["_1", "_2", "_3"]),
                csv(&["e", "f", "200"], &["_1", "_2", "_3"]),
                csv(&["g", "h", "300"], &["_1", "_2", "_3"]),
            ],
        );
        assert_eq!(rows.len(), 2);
        let expected = |f1: &str, f2: &str, f3: &str| OutRow {
            keys: vec!["_1".into(), "_2".into(), "_3".into()],
            vals: vec![s(f1), s(f2), s(f3)],
        };
        assert_eq!(rows[0], expected("a", "b", "150"));
        assert_eq!(rows[1], expected("e", "f", "200"));
    }

    #[test]
    fn missing_column_where_is_false() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._5 > 0",
            vec![csv(&["1", "2", "3"], &["_1", "_2", "_3"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn string_equality_where() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 = '1'",
            vec![
                csv(&["1", "x"], &["_1", "_2"]),
                csv(&["2", "y"], &["_1", "_2"]),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("1"));
    }

    #[test]
    fn and_or_not_where() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE (s._1 = 'a' AND s._2 = 'b') OR NOT (s._1 = 'z')",
            vec![
                csv(&["a", "b"], &["_1", "_2"]),
                csv(&["a", "c"], &["_1", "_2"]),
                csv(&["z", "b"], &["_1", "_2"]),
            ],
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].vals[0], s("a"));
        assert_eq!(rows[1].vals[0], s("a"));
    }

    #[test]
    fn arithmetic_in_where() {
        // `_1` = "10": String + Int literal via lazy parse.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 + 1 > 10",
            vec![
                csv(&["10", "x"], &["_1", "_2"]),
                csv(&["5", "y"], &["_1", "_2"]),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("10"));
    }

    #[test]
    fn in_list_honors_negated() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 IN ('a', 'b')",
            vec![
                csv(&["a"], &["_1"]),
                csv(&["b"], &["_1"]),
                csv(&["c"], &["_1"]),
            ],
        );
        assert_eq!(rows.len(), 2);
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 NOT IN ('a', 'b')",
            vec![csv(&["a"], &["_1"]), csv(&["c"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("c"));
    }

    #[test]
    fn between_honors_negated() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 BETWEEN 1 AND 10",
            vec![
                csv(&["5"], &["_1"]),
                csv(&["15"], &["_1"]),
                csv(&["x"], &["_1"]),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("5"));
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 NOT BETWEEN 1 AND 10",
            vec![csv(&["5"], &["_1"]), csv(&["15"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("15"));
    }

    #[test]
    fn is_null_on_present_string_is_false() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 IS NULL",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn projection_alias_key_over_field_name() {
        let rows = run(
            "SELECT s.Id, s.Name AS n FROM S3Object s",
            vec![csv(&["1", "alice"], &["Id", "Name"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keys, vec!["Id", "n"]);
        assert_eq!(rows[0].vals, vec![s("1"), s("alice")]);
    }

    #[test]
    fn non_boolean_where_is_false() {
        // A string in boolean position is not TRUE — filtered, no error.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._2",
            vec![csv(&["1", "x"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn is_missing_sentinels_distinguish_missing() {
        // Named ref in a headerless record: MISSING — the sentinel is true.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s.foo IS MISSING",
            vec![csv(&["1", "2"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 1);
        // Present column: the sentinel is false.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 IS MISSING",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        // Ragged USE row: header matches, field absent → MISSING.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s.Name IS MISSING",
            vec![csv(&["1"], &["Id", "Name"])],
        );
        assert_eq!(rows.len(), 1);
        let rows = run(
            "SELECT * FROM S3Object s WHERE s.Name IS NOT MISSING",
            vec![csv(&["1"], &["Id", "Name"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn is_missing_distinguishes_null_from_missing() {
        let rec = || {
            Record::Csv(Columns::from_names(
                vec![Field::Present(Value::Null), Field::Missing],
                vec!["a".into(), "b".into()],
            ))
        };
        let rows = run("SELECT * FROM S3Object s WHERE s.a IS MISSING", vec![rec()]);
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE s.a IS NOT MISSING",
            vec![rec()],
        );
        assert_eq!(rows.len(), 1);
        let rows = run("SELECT * FROM S3Object s WHERE s.b IS MISSING", vec![rec()]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn is_null_distinguishes_missing() {
        // `_5` past the row width is MISSING — IS NULL must not claim it.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._5 IS NULL",
            vec![csv(&["1", "2"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 0);
        // A present NULL is IS NULL true.
        let rec = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Null)],
            vec!["a".into()],
        ));
        let rows = run("SELECT * FROM S3Object s WHERE s.a IS NULL", vec![rec]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn and_or_short_circuit_skips_dead_arm_errors() {
        // R11: `false AND x` / `true OR x` never evaluate x — a division by
        // zero on the dead arm must not raise; the record is filtered/passes
        // instead.
        let rows = run(
            "SELECT * FROM S3Object s WHERE false AND s._1 / 0 = 0",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE true OR s._1 / 0 = 0",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        // The other arms still need the right side's truth value — a live
        // error there is unchanged.
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE NULL AND s._1 / 0 = 0").unwrap());
        assert!(engine.next(csv(&["1"], &["_1"])).is_err());
    }

    #[test]
    fn is_not_family_missing_semantics() {
        // R12: the IsNot* arms are the pure negations of the IsNull/IsTrue/
        // IsFalse cousins — a MISSING operand answers the positive form
        // false, so the negated forms are true (pinned; MISSING stays
        // distinct from Null through the dedicated sentinel operators).
        let missing = csv(&["1", "2"], &["_1", "_2"]); // `_5` is past the width
        for sql in [
            "SELECT * FROM S3Object s WHERE s._5 IS NOT NULL",
            "SELECT * FROM S3Object s WHERE s._5 IS NOT TRUE",
            "SELECT * FROM S3Object s WHERE s._5 IS NOT FALSE",
        ] {
            assert_eq!(run(sql, vec![missing.clone()]).len(), 1, "{sql}");
        }
        // A present NULL: IS NOT NULL false; IS NOT TRUE true.
        let null = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Null)],
            vec!["a".into()],
        ));
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS NOT NULL",
                vec![null.clone()]
            )
            .len(),
            0
        );
        assert_eq!(
            run("SELECT * FROM S3Object s WHERE s.a IS NOT TRUE", vec![null]).len(),
            1
        );
    }

    #[test]
    fn parens_do_not_hide_missing() {
        // `(x) IS NULL` ≡ `x IS NULL`: a parenthesized missing column stays
        // MISSING, so IS NULL must not claim it.
        let rows = run(
            "SELECT * FROM S3Object s WHERE (s._5) IS NULL",
            vec![csv(&["1", "2"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 0);
        // The MISSING sentinel sees through parentheses too.
        let rows = run(
            "SELECT * FROM S3Object s WHERE (s._5) IS MISSING",
            vec![csv(&["1", "2"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn is_true_false_family() {
        let t = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Bool(true))],
            vec!["a".into()],
        ));
        let f = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Bool(false))],
            vec!["a".into()],
        ));
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS TRUE",
                vec![t.clone()]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS TRUE",
                vec![f.clone()]
            )
            .len(),
            0
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS FALSE",
                vec![f.clone()]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS NOT FALSE",
                vec![t.clone()]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS NOT TRUE",
                vec![f.clone()]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IS NOT TRUE",
                vec![t.clone()]
            )
            .len(),
            0
        );
    }

    #[test]
    fn unary_minus_literal_folds() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 > -100",
            vec![csv(&["-50"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn string_vs_numeric_parse_failure_is_false() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 > 1",
            vec![csv(&["abc"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn bool_comparisons_only_equal_not_equal() {
        // `true < false` is not an ordering on bools → false.
        let rows = run(
            "SELECT * FROM S3Object s WHERE true < false",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE s.a = true",
            vec![csv(&["x"], &["a"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn like_percent_matches_any_sequence() {
        // `%` matches any sequence, including the empty one.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'h%'",
            vec![
                csv(&["hello", "x"], &["_1", "_2"]),
                csv(&["world", "y"], &["_1", "_2"]),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("hello"));
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE '%llo'",
            vec![csv(&["hello"], &["_1"]), csv(&["help"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE '%'",
            vec![csv(&[""], &["_1"]), csv(&["anything"], &["_1"])],
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn like_underscore_matches_exactly_one_char() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'h_llo'",
            vec![
                csv(&["hello"], &["_1"]),
                csv(&["hllo"], &["_1"]),
                csv(&["helloo"], &["_1"]),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("hello"));
        // `_` is exactly one character: `a` passes, `ab` and `''` do not.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE '_'",
            vec![
                csv(&["a"], &["_1"]),
                csv(&["ab"], &["_1"]),
                csv(&[""], &["_1"]),
            ],
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_is_case_sensitive() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'h%'",
            vec![csv(&["HELLO"], &["_1"]), csv(&["hello"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("hello"));
    }

    #[test]
    fn like_without_wildcards_is_exact() {
        // A pattern with no `%`/`_` is exact equality.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'hello'",
            vec![csv(&["hello"], &["_1"]), csv(&["hell"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("hello"));
    }

    #[test]
    fn like_escape_makes_wildcards_literal() {
        // ESCAPE '\' turns `\%`/`\_` into literal characters.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'a\\%b' ESCAPE '\\'",
            vec![csv(&["a%b"], &["_1"]), csv(&["axb"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("a%b"));
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'a\\_b' ESCAPE '\\'",
            vec![csv(&["a_b"], &["_1"]), csv(&["ab"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("a_b"));
    }

    #[test]
    fn like_escape_of_the_escape_char() {
        // The escape char escapes itself: pattern `a\\b` with ESCAPE '\' is
        // the literal 3-char text `a\b`.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 LIKE 'a\\\\b' ESCAPE '\\'",
            vec![csv(&["a\\b"], &["_1"]), csv(&["ab"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("a\\b"));
    }

    #[test]
    fn like_escape_must_be_one_char() {
        // sqlparser 0.62 parses any literal string after ESCAPE; standard
        // SQL takes exactly one character. Request-level since the walk began
        // consulting the evaluator's own check: the two-char form is a 400,
        // and the runtime arm below is the backstop (the parse half can no
        // longer reach it, so the plan is built by hand — the same shape the
        // parity corpus below uses).
        assert_eq!(
            parse("SELECT * FROM S3Object s WHERE s._1 LIKE 'a\\%b' ESCAPE '\\%'")
                .unwrap_err()
                .to_string(),
            "S3 select: ESCAPE must be a single character"
        );
        let mut engine = Engine::new(plan_with_where(like_expr(
            "_1",
            false,
            "a%b",
            Some(AstValue::SingleQuotedString("\\%".into()).into()),
        )));
        match engine.next(csv(&["a%b"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "ESCAPE must be a single character"),
            other => panic!("expected one-char escape error, got {other:?}"),
        }
    }

    #[test]
    fn like_consecutive_percent_runs_collapse() {
        // A `%` run is one star — `%%` is not a literal-percent suffix.
        let rows = |sql: &str| run(sql, vec![csv(&["x"], &["_1"])]).len();
        assert_eq!(rows("SELECT * FROM S3Object s WHERE 'hello' LIKE '%%'"), 1);
        assert_eq!(
            rows("SELECT * FROM S3Object s WHERE 'hello' LIKE '%%llo%%'"),
            1
        );
        assert_eq!(rows("SELECT * FROM S3Object s WHERE 'a' LIKE '%%%'"), 1);
        assert_eq!(
            rows("SELECT * FROM S3Object s WHERE 'a-b' LIKE '%%a%%b%%'"),
            1
        );
        // A star before a subject's literal `%` consumes it, and a star
        // also covers text with no literal `%` at all.
        assert_eq!(rows("SELECT * FROM S3Object s WHERE '50%' LIKE '50%%'"), 1);
        assert_eq!(rows("SELECT * FROM S3Object s WHERE '500' LIKE '50%%'"), 1);
        // Escaped literal `%` followed by a star stays literal.
        assert_eq!(
            rows("SELECT * FROM S3Object s WHERE '50%' LIKE '50\\%%' ESCAPE '\\'"),
            1
        );
    }

    #[test]
    fn like_not_negates() {
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._1 NOT LIKE 'd%'",
            vec![csv(&["abc"], &["_1"]), csv(&["dab"], &["_1"])],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals[0], s("abc"));
    }

    #[test]
    fn like_missing_subject_is_false() {
        // `_5` is past the row width: MISSING → false, like every
        // non-string operand.
        let rows = run(
            "SELECT * FROM S3Object s WHERE s._5 LIKE 'x%'",
            vec![csv(&["1", "2"], &["_1", "_2"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn like_non_string_operands_are_false() {
        // Numbers, bools and NULL never match — no coercion, and the
        // negated forms stay false too (R13: the negation must not flip the
        // non-string mismatch to true).
        let rows = run(
            "SELECT * FROM S3Object s WHERE 5 LIKE '5%'",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE '5' LIKE 5",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE NULL LIKE 'x%'",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE 5 NOT LIKE '5%'",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
        let rows = run(
            "SELECT * FROM S3Object s WHERE NULL NOT LIKE 'x%'",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn like_empty_pattern_and_subject_edges() {
        // Empty pattern matches only the empty subject; `%` matches the
        // empty subject; `_` needs exactly one character.
        let rows = |sql| run(sql, vec![csv(&["x"], &["_1"])]).len();
        assert_eq!(rows("SELECT * FROM S3Object s WHERE '' LIKE ''"), 1);
        assert_eq!(rows("SELECT * FROM S3Object s WHERE 'a' LIKE ''"), 0);
        assert_eq!(rows("SELECT * FROM S3Object s WHERE '' LIKE '%'"), 1);
        assert_eq!(rows("SELECT * FROM S3Object s WHERE '' LIKE '_'"), 0);
    }

    #[test]
    fn like_percent_heavy_pattern_is_polynomial() {
        // (%a)×300 vs 600 chars ≈ 3.6·10^5 DP cells; a backtracking matcher
        // would wander 2^300 paths (review 2026-09-05 #5).
        let pattern = "%a".repeat(300);
        assert!(
            like_match(
                &format!("{}{}", "b".repeat(300), "a".repeat(300)),
                &pattern,
                None
            )
            .unwrap()
        );
        // The trailing `b`s cannot be absorbed: no star follows the last
        // literal `a`.
        assert!(
            !like_match(
                &format!("{}{}", "a".repeat(300), "b".repeat(300)),
                &pattern,
                None
            )
            .unwrap()
        );
    }

    #[test]
    fn like_cell_cap_refuses_huge_patterns() {
        // R7: the 10⁷-cell ceiling — a 100k-token pattern × 200-char text
        // (2×10⁷ cells) is refused with a value error instead of pinning a
        // spawn_blocking worker for seconds-to-minutes.
        let pattern = "%a".repeat(50_000);
        match like_match(&"a".repeat(200), &pattern, None) {
            Err(Error::Value(m)) => assert_eq!(m, "LIKE pattern too large"),
            other => panic!("expected cell-cap error, got {other:?}"),
        }
    }

    /// Builds `Expr::Function` with a single-part name — the shape the parser
    /// produces for `LOWER(x)`, `count(x)` and the request sentinel alike.
    fn func(name: &str) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }

    #[test]
    fn unsupported_form_is_variant_based_not_text_based() {
        // `s.CAST` — an identifier whose *text* is a keyword — is supported.
        let compound = Expr::CompoundIdentifier(vec![Ident::new("s"), Ident::new("CAST")]);
        assert!(unsupported_form(&compound, "sentinel").is_none());
        // `CAST(x AS INT)` — the *variant* — is not, and is named statically.
        // (`Expr::Cast` in 0.62 has five fields; `array` is the one easy to miss.)
        let cast = Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("x"))),
            data_type: DataType::Int(None),
            array: false,
            format: None,
        };
        match unsupported_form(&cast, "sentinel") {
            Some(UnsupportedForm::Expression(n)) => assert_eq!(n, "CAST"),
            other => panic!("expected Expression(CAST), got {other:?}"),
        }
    }

    #[test]
    fn unsupported_form_admits_aggregates_and_the_request_sentinel() {
        assert!(unsupported_form(&func("count"), "sentinel").is_none());
        assert!(unsupported_form(&func("sum"), "sentinel").is_none());
        assert!(unsupported_form(&func("sentinel"), "sentinel").is_none());
        match unsupported_form(&func("LOWER"), "sentinel") {
            Some(UnsupportedForm::Function(n)) => assert_eq!(n, "LOWER"),
            other => panic!("expected Function(LOWER), got {other:?}"),
        }
    }

    #[test]
    fn unsupported_form_diagnostics_carry_their_text() {
        assert_eq!(
            UnsupportedForm::Expression("CASE".into()).to_string(),
            "unsupported expression: CASE"
        );
        assert_eq!(
            UnsupportedForm::Function("LOWER".into()).to_string(),
            "unsupported function: LOWER"
        );
        // The two operand verdicts carry the evaluator's own text verbatim —
        // a prefix here would be a diagnostic nothing else spells.
        assert_eq!(
            UnsupportedForm::Literal("X'41'".into()).to_string(),
            "X'41'"
        );
        assert_eq!(
            UnsupportedForm::Malformed("invalid numeric value: 1e999".into()).to_string(),
            "invalid numeric value: 1e999"
        );
    }

    /// Literal operands are judged by the converter itself, so the request-level
    /// refusal cannot drift from the value the evaluator would have produced.
    /// Before this, every one of these parsed, streamed, and failed per row —
    /// the acceptance-surface hole the walk exists to close.
    ///
    /// The byte/raw-string rows at the top are the same family one step worse:
    /// until `S3SelectDialect` reported `GenericDialect`'s `TypeId` (see the
    /// `dialect()` override), `dialect_of!`'s byte/raw prefix arms
    /// (`tokenizer.rs:1085`/`1125`) read false, so `B'1'` never became a
    /// literal at all — it tokenized as the identifier `B` aliased `'1'` and
    /// the query returned a MISSING column named `1` (an empty CSV field)
    /// instead of erroring.
    #[test]
    fn literal_operand_defects_are_request_level() {
        let cases: &[(&str, &str)] = &[
            (
                "SELECT B'1' FROM S3Object s",
                "S3 select: unsupported: B'1'",
            ),
            // The lowercase prefix is the same token: sqlparser's `Display`
            // names the byte form in uppercase, as it does for `0x41` below.
            (
                "SELECT b'1' FROM S3Object s",
                "S3 select: unsupported: B'1'",
            ),
            (
                "SELECT R'1' FROM S3Object s",
                "S3 select: unsupported: R'1'",
            ),
            // The double-quoted and triple-quoted spellings reach the same
            // catch-all. `B'''1'''` renders as `B''1''` — sqlparser's
            // triple-quote `Display`, observed, not predicted.
            (
                "SELECT B\"1\" FROM S3Object s",
                "S3 select: unsupported: B\"1\"",
            ),
            (
                "SELECT R\"1\" FROM S3Object s",
                "S3 select: unsupported: R\"1\"",
            ),
            (
                "SELECT R'''1''' FROM S3Object s",
                "S3 select: unsupported: R'''1'''",
            ),
            (
                "SELECT B'''1''' FROM S3Object s",
                "S3 select: unsupported: B''1''",
            ),
            (
                "SELECT X'41' FROM S3Object s",
                "S3 select: unsupported: X'41'",
            ),
            // `0x41` is the same literal spelled differently; sqlparser
            // normalizes it, so both name the hex form.
            (
                "SELECT 0x41 FROM S3Object s",
                "S3 select: unsupported: X'41'",
            ),
            (
                "SELECT N'x' FROM S3Object s",
                "S3 select: unsupported: N'x'",
            ),
            (
                "SELECT $$x$$ FROM S3Object s",
                "S3 select: unsupported: $$x$$",
            ),
            (
                "SELECT U&'x' FROM S3Object s",
                "S3 select: unsupported: U&'x'",
            ),
            (
                "SELECT 1e999 FROM S3Object s",
                "S3 select: invalid numeric value: 1e999",
            ),
            (
                "SELECT 99999999999999999999999999999 FROM S3Object s",
                "S3 select: numeric value exceeds 28-digit precision: 99999999999999999999999999999",
            ),
            (
                "SELECT s.a FROM S3Object s WHERE s.a = X'41'",
                "S3 select: unsupported: X'41'",
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(parse(sql).unwrap_err().to_string(), *expected, "for {sql}");
        }
    }

    /// The runtime half of the pair above, on hand-built plans because `parse`
    /// refuses all of it now: the same converter text on its own channel, which
    /// is what the two rewritten `like_escape_*` tests assert for `ESCAPE`.
    #[test]
    fn literal_operand_defects_keep_their_runtime_text() {
        let hex = Expr::Value(AstValue::HexStringLiteral("41".into()).into());
        match Engine::new(plan_with_where(hex)).next(csv(&["x"], &["_1"])) {
            Err(Error::Unsupported(m)) => assert_eq!(m, "X'41'"),
            other => panic!("expected the hex refusal, got {other:?}"),
        }
        let big = Expr::Value(AstValue::Number("1e999".into(), false).into());
        match Engine::new(plan_with_where(big)).next(csv(&["x"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: 1e999"),
            other => panic!("expected the numeric value error, got {other:?}"),
        }
    }

    /// An unnamed form is echoed, so the echo is scrubbed and bounded: the
    /// request's sentinel name never reaches the client, and a 256 KiB
    /// expression cannot become a 256 KiB message.
    #[test]
    fn rendered_diagnostics_scrub_the_sentinel_and_are_bounded() {
        let sentinel = SentinelNames::mint();
        let name = sentinel.is_missing();
        // The dialect rewrites `IS MISSING` wherever it appears, so a rejected
        // container's rendered text can carry the sentinel inside it.
        let nested = Expr::Tuple(vec![func(&name)]);
        match unsupported_form(&nested, &name) {
            Some(UnsupportedForm::Expression(text)) => {
                assert!(!text.contains(&name), "sentinel leaked: {text}");
                assert!(text.contains("IS MISSING"), "{text}");
            }
            other => panic!("expected Expression, got {other:?}"),
        }
        // Bounded on a char boundary: 300 chars in, the cap plus the ellipsis
        // out (the tuple's own parens included).
        let long = Expr::Tuple(vec![Expr::Identifier(Ident::new("a".repeat(300)))]);
        match unsupported_form(&long, &name) {
            Some(UnsupportedForm::Expression(text)) => {
                assert_eq!(text.chars().count(), RENDERED_DIAGNOSTIC + 3, "{text}");
                assert!(text.ends_with("..."), "{text}");
            }
            other => panic!("expected Expression, got {other:?}"),
        }
    }

    /// A plan built without `parse` — the validator refuses the forms these
    /// tests drive, so the engine arms cannot be reached through a query.
    fn plan(projections: Vec<Projection>, where_expr: Option<Expr>) -> QueryPlan {
        QueryPlan {
            from: FromClause {
                segments: vec![],
                alias: Some("s".into()),
            },
            projections,
            where_expr,
            limit: None,
            aggregates: false,
            missing: SentinelNames::mint(),
        }
    }

    /// The common shape: project `_1` and filter on `where_expr`.
    fn plan_with_where(where_expr: Expr) -> QueryPlan {
        plan(
            vec![Projection::Item {
                expr: col(),
                alias: None,
            }],
            Some(where_expr),
        )
    }

    /// The parity corpus: one row per form [`unsupported_form`] names, each
    /// row asserting that *both* layers consulting the predicate — the
    /// parse-time walk behind [`parse`] and [`Engine::next`] — refuse that
    /// form, with the one expected string the row carries. Neither layer
    /// passes on its own: a predicate that renames or drops a form fails the
    /// row twice over, which is what keeps the two from drifting.
    ///
    /// `subquery` is the one named form with no row here: `reject_subqueries`
    /// (`sql.rs`) refuses `Subquery`/`InSubquery`/`Exists` on every expression
    /// before either layer can see one, so its refusal is pinned at that layer
    /// (the `unsupported: subquery` assertions in `sql.rs`) rather than twice.
    ///
    /// A row's SQL text and its hand-built `Expr` are two *spellings* of one
    /// form, not two equal ASTs: what the row pins is that both produce the
    /// same expected string. The three fields are asserted as:
    ///
    /// - parse: `parse(sql)` answers `S3 select: unsupported: {expected}`
    ///   (request-level, before any byte is streamed);
    /// - eval: a plan whose WHERE is the hand-built `Expr` answers
    ///   `Error::Unsupported(expected)` on the first record.
    ///
    /// The hand-built `Expr`s are the eval half's only channel: `parse`
    /// refuses every one of these forms, so no end-to-end query can reach
    /// `eval` with one (design: "the `eval` side of the corpus must call
    /// `eval`/`unsupported_form` directly with hand-built `Expr`s").
    ///
    /// The two infix-only rows (`LIKE ANY`, `ILIKE`) are reached through the
    /// same catch-all as every other row, so they need no dedicated tests of
    /// their own — the ones they had were folded into this table, which covers
    /// the whole named set at once, on both layers.
    #[test]
    fn eval_refuses_every_named_form_the_predicate_knows() {
        let cases: &[(&str, Expr, &str)] = &[
            (
                "SELECT CASE WHEN s.a > 1 THEN 1 ELSE 0 END FROM S3Object s",
                Expr::Case {
                    case_token: AttachedToken::empty(),
                    end_token: AttachedToken::empty(),
                    operand: None,
                    conditions: vec![CaseWhen {
                        condition: col(),
                        result: str_lit("x"),
                    }],
                    else_result: None,
                },
                "unsupported expression: CASE",
            ),
            (
                "SELECT CAST(s.a AS INT) FROM S3Object s",
                cast(CastKind::Cast),
                "unsupported expression: CAST",
            ),
            (
                "SELECT TRY_CAST(s.a AS INT) FROM S3Object s",
                cast(CastKind::TryCast),
                "unsupported expression: TRY_CAST",
            ),
            (
                "SELECT SAFE_CAST(s.a AS INT) FROM S3Object s",
                cast(CastKind::SafeCast),
                "unsupported expression: SAFE_CAST",
            ),
            (
                "SELECT SUBSTRING(s.a, 1) FROM S3Object s",
                Expr::Substring {
                    expr: Box::new(col()),
                    substring_from: None,
                    substring_for: None,
                    special: false,
                    shorthand: false,
                },
                "unsupported expression: SUBSTRING",
            ),
            (
                "SELECT TRIM(s.a) FROM S3Object s",
                Expr::Trim {
                    trim_where: None,
                    trim_what: None,
                    expr: Box::new(col()),
                    trim_characters: None,
                },
                "unsupported expression: TRIM",
            ),
            (
                "SELECT EXTRACT(MONTH FROM s.a) FROM S3Object s",
                Expr::Extract {
                    field: DateTimeField::Month,
                    syntax: ExtractSyntax::From,
                    expr: Box::new(col()),
                },
                "unsupported expression: EXTRACT",
            ),
            (
                "SELECT CEIL(s.a TO YEAR) FROM S3Object s",
                Expr::Ceil {
                    expr: Box::new(col()),
                    field: CeilFloorKind::DateTimeField(DateTimeField::Year),
                },
                "unsupported expression: CEIL",
            ),
            (
                "SELECT FLOOR(s.a TO YEAR) FROM S3Object s",
                Expr::Floor {
                    expr: Box::new(col()),
                    field: CeilFloorKind::DateTimeField(DateTimeField::Year),
                },
                "unsupported expression: FLOOR",
            ),
            (
                "SELECT POSITION('x' IN s.a) FROM S3Object s",
                Expr::Position {
                    expr: Box::new(str_lit("x")),
                    r#in: Box::new(col()),
                },
                "unsupported expression: POSITION",
            ),
            (
                "SELECT INTERVAL '1 day' FROM S3Object s",
                Expr::Interval(Interval {
                    value: Box::new(str_lit("1 day")),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                }),
                "unsupported expression: INTERVAL",
            ),
            (
                "SELECT DATE '2020-01-01' FROM S3Object s",
                Expr::TypedString(TypedString {
                    data_type: DataType::Date,
                    value: AstValue::SingleQuotedString("2020-01-01".into()).into(),
                    uses_odbc_syntax: false,
                }),
                "unsupported expression: TYPED STRING",
            ),
            (
                "SELECT s.a FROM S3Object s WHERE s._1 IS DISTINCT FROM 'x'",
                Expr::IsDistinctFrom(Box::new(col()), Box::new(str_lit("x"))),
                "unsupported expression: IS DISTINCT FROM",
            ),
            // Infix-only forms, both refused rather than silently run as
            // something else: `LIKE ANY` (Snowflake, outside the AWS surface —
            // plain single-pattern LIKE would be a silent semantic change) and
            // `ILIKE` (AWS S3 Select has no case-insensitive LIKE, so a
            // case-sensitive LIKE would be a silent one too — review
            // 2026-09-05b); both `Like` and `ILike` carry five fields.
            (
                "SELECT * FROM S3Object s WHERE s._1 LIKE ANY 'x%'",
                Expr::Like {
                    negated: false,
                    any: true,
                    expr: Box::new(col()),
                    pattern: Box::new(str_lit("x%")),
                    escape_char: None,
                },
                "unsupported expression: LIKE ANY",
            ),
            (
                "SELECT * FROM S3Object s WHERE s._1 ILIKE 'X'",
                Expr::ILike {
                    negated: false,
                    any: false,
                    expr: Box::new(col()),
                    pattern: Box::new(str_lit("X")),
                    escape_char: None,
                },
                "unsupported expression: ILIKE",
            ),
            (
                "SELECT LOWER(s.a) FROM S3Object s",
                func("LOWER"),
                "unsupported function: LOWER",
            ),
            (
                "SELECT COALESCE(s.a, 'x') FROM S3Object s",
                func("COALESCE"),
                "unsupported function: COALESCE",
            ),
            (
                "SELECT unknown_fn(s._1) FROM S3Object s",
                func("unknown_fn"),
                "unsupported function: unknown_fn",
            ),
            (
                "SELECT s.a FROM S3Object s WHERE NOW() = s.a",
                func("NOW"),
                "unsupported function: NOW",
            ),
        ];
        for (sql, expr, expected) in cases {
            // Parse half: the walk behind `parse` refuses the request-level
            // SQL with the shared predicate's own text.
            assert_eq!(
                parse(sql).unwrap_err().to_string(),
                format!("S3 select: unsupported: {expected}"),
                "parse half for {sql}"
            );
            // Eval half: the same form, hand-built, refused by the evaluator.
            let mut engine = Engine::new(plan_with_where(expr.clone()));
            match engine.next(csv(&["x"], &["_1"])) {
                Err(Error::Unsupported(m)) => assert_eq!(m, *expected, "eval half for {sql}"),
                other => panic!("expected {sql} refused at eval, got {other:?}"),
            }
        }
    }

    /// The subject expression of the corpus above: the WHERE column `_1` of
    /// the `csv(&["x"], &["_1"])` record it is run over.
    fn col() -> Expr {
        Expr::Identifier(Ident::new("_1"))
    }

    fn str_lit(s: &str) -> Expr {
        Expr::Value(AstValue::SingleQuotedString(s.into()).into())
    }

    /// `CAST`/`TRY_CAST`/`SAFE_CAST` are one `Expr::Cast` variant differing
    /// only in its `CastKind`; the operand and target type never reach the
    /// classification, which is variant-based.
    fn cast(kind: CastKind) -> Expr {
        Expr::Cast {
            kind,
            expr: Box::new(col()),
            data_type: DataType::Int(None),
            array: false,
            format: None,
        }
    }

    /// A `Like` node over `subject`, parameterised by the three fields the
    /// tests vary — the node's other two are always the same here.
    fn like_expr(
        subject: &str,
        any: bool,
        pattern: &str,
        escape_char: Option<ValueWithSpan>,
    ) -> Expr {
        Expr::Like {
            negated: false,
            any,
            expr: Box::new(Expr::Identifier(Ident::new(subject))),
            pattern: Box::new(str_lit(pattern)),
            escape_char,
        }
    }

    /// The drift guard: the predicate admits the aggregate names (they belong to
    /// the scan-accumulate path), so an aggregate reaching `eval`'s Function arm
    /// is precisely the case the two sides disagree on — the arm must keep its
    /// internal error, proving the backstop still exists.
    #[test]
    fn function_arm_drift_guard_keeps_the_internal_error() {
        let mut engine = Engine::new(plan(
            vec![Projection::Item {
                expr: func("sum"),
                alias: None,
            }],
            None,
        ));
        match engine.next(csv(&["x"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "internal: unexpected aggregate call"),
            other => panic!("expected the internal error, got {other:?}"),
        }
    }

    #[test]
    fn count_star_counts_every_row() {
        let row = run_agg(
            "SELECT count(*) FROM S3Object s",
            vec![
                csv(&["1", "x"], &["_1", "_2"]),
                csv(&["2", "y"], &["_1", "_2"]),
            ],
        );
        assert_eq!(row.keys, vec!["count(*)"]);
        assert_eq!(row.vals, vec![Field::Present(Value::Int(2))]);
    }

    #[test]
    fn count_expr_missing_and_null_are_zero() {
        // `_5` past the row width is MISSING for every row.
        let row = run_agg(
            "SELECT count(s._5) FROM S3Object s",
            vec![
                csv(&["1", "2", "3"], &["_1", "_2", "_3"]),
                csv(&["1", "2", "3"], &["_1", "_2", "_3"]),
            ],
        );
        assert_eq!(row.vals, vec![Field::Present(Value::Int(0))]);
        // A present NULL does not count either.
        let rec = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Null)],
            vec!["a".into()],
        ));
        let row = run_agg("SELECT count(s.a) FROM S3Object s", vec![rec]);
        assert_eq!(row.vals, vec![Field::Present(Value::Int(0))]);
    }

    #[test]
    fn sum_parses_numeric_strings() {
        let row = run_agg(
            "SELECT sum(s._3) FROM S3Object s",
            vec![
                csv(&["1", "x", "10"], &["_1", "_2", "_3"]),
                csv(&["2", "y", "20"], &["_1", "_2", "_3"]),
            ],
        );
        assert_eq!(
            row.vals,
            vec![Field::Present(Value::Decimal(Decimal::from(30)))]
        );
    }

    #[test]
    fn sum_non_numeric_value_errors() {
        // AWS cast-fails: a non-numeric contributor in a SUM is a value
        // error, never a silent skip (controller ruling, Task 7).
        let mut engine = Engine::new(parse("SELECT sum(s._3) FROM S3Object s").unwrap());
        assert_eq!(
            engine
                .next(csv(&["1", "x", "10"], &["_1", "_2", "_3"]))
                .unwrap(),
            None
        );
        match engine.next(csv(&["2", "y", "x"], &["_1", "_2", "_3"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: x"),
            other => panic!("expected invalid numeric error, got {other:?}"),
        }
    }

    #[test]
    fn avg_is_decimal_scale_ten() {
        // Exact average: 1.5.
        let row = run_agg(
            "SELECT avg(s._1) FROM S3Object s",
            vec![csv(&["1"], &["_1"]), csv(&["2"], &["_1"])],
        );
        assert_eq!(
            row.vals,
            vec![Field::Present(Value::Decimal(Decimal::new(15, 1)))]
        );
        // Non-terminating: 5/3 rounds to scale 10.
        let row = run_agg(
            "SELECT avg(s._1) FROM S3Object s",
            vec![
                csv(&["1"], &["_1"]),
                csv(&["2"], &["_1"]),
                csv(&["2"], &["_1"]),
            ],
        );
        assert_eq!(
            row.vals,
            vec![Field::Present(Value::Decimal(Decimal::new(
                16666666667,
                10
            )))]
        );
    }

    #[test]
    fn min_max_numeric_strings_are_decimals() {
        // First contributor parses → the column is numeric: extrema are
        // parsed Decimals, so ['10','2'] → min 2, max 10 (not "10").
        let row = run_agg(
            "SELECT min(s._1), max(s._1) FROM S3Object s",
            vec![csv(&["10"], &["_1"]), csv(&["2"], &["_1"])],
        );
        assert_eq!(
            row.vals,
            vec![
                Field::Present(Value::Decimal(Decimal::from(2))),
                Field::Present(Value::Decimal(Decimal::from(10))),
            ]
        );
    }

    #[test]
    fn min_max_strings_lexicographic() {
        // Non-numeric strings stay a string column: strict lexicographic.
        let row = run_agg(
            "SELECT min(s._2), max(s._2) FROM S3Object s",
            vec![
                csv(&["1", "alice"], &["_1", "_2"]),
                csv(&["2", "carol"], &["_1", "_2"]),
                csv(&["3", "bob"], &["_1", "_2"]),
            ],
        );
        assert_eq!(row.keys, vec!["min(s._2)", "max(s._2)"]);
        assert_eq!(row.vals, vec![s("alice"), s("carol")]);
    }

    #[test]
    fn min_max_numeric_invalid_late_value_errors() {
        // A numeric column hitting a non-parseable string cast-fails with a
        // value error (the aggregate never returns a wrong extrema).
        let mut engine = Engine::new(parse("SELECT min(s._1) FROM S3Object s").unwrap());
        assert_eq!(engine.next(csv(&["10"], &["_1"])).unwrap(), None);
        match engine.next(csv(&["x"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: x"),
            other => panic!("expected invalid numeric error, got {other:?}"),
        }
    }

    #[test]
    fn min_max_first_contributor_over_precision_is_an_error() {
        // A 29+ digit first contributor hits the 28-digit precision contract
        // — the error surfaces on the spot; it is never demoted to a string
        // column, which would silently answer a lexicographic "2".
        let big = "9".repeat(29);
        let mut engine = Engine::new(parse("SELECT min(s._1) FROM S3Object s").unwrap());
        match engine.next(csv(&[big.as_str()], &["_1"])) {
            Err(Error::Value(m)) => {
                assert_eq!(
                    m,
                    format!("numeric value exceeds 28-digit precision: {big}")
                )
            }
            other => panic!("expected precision error, got {other:?}"),
        }
    }

    #[test]
    fn min_max_exponent_first_contributor_is_a_value_error() {
        // `1e30` is numeric-shaped (a finite f64) but exceeds the 96-bit
        // spine: the parse error surfaces on the spot — never a demotion to
        // a lexicographic column, which would silently answer "1e30" (a
        // string min loses to "2" lexicographically).
        let mut engine = Engine::new(parse("SELECT min(s._1) FROM S3Object s").unwrap());
        match engine.next(csv(&["1e30"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: 1e30"),
            other => panic!("expected value error, got {other:?}"),
        }
    }

    #[test]
    fn min_max_nan_inf_stay_string_columns() {
        // NaN/inf parse as f64 but are not finite — not numeric-shaped, so
        // the column stays strict-lexicographic (no precision error, no
        // cast failure, and "20" ranks as text: "2" < "N" < "i").
        let row = run_agg(
            "SELECT min(s._1), max(s._1) FROM S3Object s",
            vec![
                csv(&["NaN"], &["_1"]),
                csv(&["inf"], &["_1"]),
                csv(&["20"], &["_1"]),
            ],
        );
        assert_eq!(row.keys, vec!["min(s._1)", "max(s._1)"]);
        assert_eq!(row.vals, vec![s("20"), s("inf")]);
    }

    #[test]
    fn zero_row_object_aggregates() {
        // No records at all: COUNT(*) = 0, every other aggregate = NULL.
        let row = run_agg(
            "SELECT count(*), sum(s._1), avg(s._1), min(s._1), max(s._1) FROM S3Object s",
            vec![],
        );
        assert_eq!(
            row.vals,
            vec![
                Field::Present(Value::Int(0)),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
            ]
        );
    }

    #[test]
    fn all_null_row_aggregates() {
        // One all-NULL row: COUNT(*) = 1 (a row), COUNT(expr) = 0, extrema
        // and sums stay NULL.
        let rec = Record::Csv(Columns::from_names(
            vec![Field::Present(Value::Null), Field::Present(Value::Null)],
            vec!["a".into(), "b".into()],
        ));
        let row = run_agg(
            "SELECT count(*), count(s.a), sum(s.b), avg(s.b), min(s.a), max(s.a) FROM S3Object s",
            vec![rec],
        );
        assert_eq!(
            row.vals,
            vec![
                Field::Present(Value::Int(1)),
                Field::Present(Value::Int(0)),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
                Field::Present(Value::Null),
            ]
        );
    }

    #[test]
    fn sum_and_avg_over_all_missing_is_null() {
        // `_5` past the row width: no contributors → NULL, not 0.
        let row = run_agg(
            "SELECT sum(s._5), avg(s._5) FROM S3Object s",
            vec![csv(&["1", "2", "3"], &["_1", "_2", "_3"])],
        );
        assert_eq!(
            row.vals,
            vec![Field::Present(Value::Null), Field::Present(Value::Null)]
        );
    }

    #[test]
    fn aggregate_where_counts_only_passing_rows() {
        let row = run_agg(
            "SELECT count(*) FROM S3Object s WHERE s._1 = '1'",
            vec![
                csv(&["1", "x"], &["_1", "_2"]),
                csv(&["2", "y"], &["_1", "_2"]),
            ],
        );
        assert_eq!(row.vals, vec![Field::Present(Value::Int(1))]);
    }

    #[test]
    fn aggregate_alias_is_the_key() {
        let row = run_agg(
            "SELECT count(*) AS n FROM S3Object s",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(row.keys, vec!["n"]);
        assert_eq!(row.vals, vec![Field::Present(Value::Int(1))]);
    }

    #[test]
    fn aggregate_next_emits_no_rows() {
        // Aggregate mode accumulates across every `next`; rows only exist at
        // `finish` — filtered or not.
        let mut engine = Engine::new(parse("SELECT count(*) FROM S3Object s").unwrap());
        assert_eq!(engine.next(csv(&["1"], &["_1"])).unwrap(), None);
        assert_eq!(engine.next(csv(&["2"], &["_1"])).unwrap(), None);
    }

    #[test]
    fn multi_aggregate_row_shapes() {
        // Bare forms compose into one row, in projection order.
        let row = run_agg(
            "SELECT count(*), count(s._1), sum(s._3), avg(s._3), min(s._2) FROM S3Object s",
            vec![
                csv(&["1", "x", "10"], &["_1", "_2", "_3"]),
                csv(&["1", "y", "20"], &["_1", "_2", "_3"]),
            ],
        );
        assert_eq!(
            row.keys,
            vec![
                "count(*)",
                "count(s._1)",
                "sum(s._3)",
                "avg(s._3)",
                "min(s._2)"
            ]
        );
        assert_eq!(
            row.vals,
            vec![
                Field::Present(Value::Int(2)),
                Field::Present(Value::Int(2)),
                Field::Present(Value::Decimal(Decimal::from(30))),
                Field::Present(Value::Decimal(Decimal::from(15))),
                s("x"),
            ]
        );
    }

    #[test]
    fn division_by_zero_exact_message() {
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 / 0 > 1").unwrap());
        match engine.next(csv(&["1"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "division by zero"),
            other => panic!("expected division by zero, got {other:?}"),
        }
    }

    #[test]
    fn divide_is_decimal_scale_ten() {
        let rows = run(
            "SELECT s._1 / 2 FROM S3Object s",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::new(5, 1)))
        );
        let rows = run(
            "SELECT s._1 / 3 FROM S3Object s",
            vec![csv(&["1"], &["_1"])],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::new(3333333333, 10)))
        );
    }

    #[test]
    fn int_overflow_promotes_to_decimal() {
        let rows = run(
            "SELECT 9223372036854775807 + 1 FROM S3Object s",
            vec![csv(&["x"], &["_1"])],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::from(9223372036854775808u64)))
        );
    }

    #[test]
    fn modulo_int_only() {
        let rows = run(
            "SELECT s._1 % 2 FROM S3Object s",
            vec![csv(&["5"], &["_1"])],
        );
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(1)));
        // Decimal operands are rejected: `%` is integral-only.
        let mut engine = Engine::new(parse("SELECT 5.5 % 2 FROM S3Object s").unwrap());
        match engine.next(csv(&["x"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: 5.5"),
            other => panic!("expected integral-only modulo error, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_on_non_numeric_is_an_error() {
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 + 1 > 10").unwrap());
        match engine.next(csv(&["abc"], &["_1"])) {
            Err(Error::Value(m)) => assert_eq!(m, "invalid numeric value: abc"),
            other => panic!("expected arithmetic value error, got {other:?}"),
        }
    }

    #[test]
    fn finish_returns_none() {
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s").unwrap());
        assert_eq!(engine.finish().unwrap(), None);
    }

    #[test]
    fn use_mode_header_lookup_is_case_insensitive() {
        let rows = run(
            "SELECT s.id FROM S3Object s",
            vec![csv(&["1", "alice"], &["Id", "Name"])],
        );
        assert_eq!(rows[0].keys, vec!["id"]);
        assert_eq!(rows[0].vals[0], s("1"));
    }

    #[test]
    fn use_mode_missing_header_errors() {
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s WHERE s.nope = 'x'").unwrap());
        match engine.next(csv(&["1", "alice"], &["Id", "Name"])) {
            Err(Error::MissingHeader(m)) => assert_eq!(m, "nope"),
            other => panic!("expected missing header, got {other:?}"),
        }
    }

    #[test]
    fn case_insensitive_duplicate_header_is_ambiguous() {
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s WHERE s.x = '1'").unwrap());
        match engine.next(csv(&["1"], &["X", "x"])) {
            Err(Error::Ambiguous(m)) => assert_eq!(m, "x"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn limit_stops_even_when_query_would_pass_more() {
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 > 0 LIMIT 1").unwrap());
        assert!(engine.next(csv(&["1"], &["_1"])).unwrap().is_some());
        assert!(engine.next(csv(&["2"], &["_1"])).unwrap().is_none());
        assert!(engine.next(csv(&["3"], &["_1"])).unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // Parquet column resolution (Task 11).
    // ------------------------------------------------------------------

    fn parquet(fields: Vec<Field>, names: &[&str]) -> Record {
        Record::Parquet(Columns::from_names(
            fields,
            names.iter().map(|n| n.to_string()).collect(),
        ))
    }

    #[test]
    fn parquet_lookup_finds_projected_columns() {
        let rec = parquet(
            vec![
                Field::Present(Value::Int(1)),
                Field::Present(Value::Decimal(Decimal::new(1234, 2))),
                Field::Missing,
            ],
            &["id", "price", "gone"],
        );
        // Unquoted names are case-insensitive per the identifier rules.
        assert_eq!(
            column_value(&rec, "ID", NameStyle::Bare).unwrap(),
            Some(Field::Present(Value::Int(1)))
        );
        assert_eq!(
            column_value(&rec, "price", NameStyle::Bare).unwrap(),
            Some(Field::Present(Value::Decimal(Decimal::new(1234, 2))))
        );
        // `_N` positional spine works like CSV.
        assert_eq!(
            column_value(&rec, "_2", NameStyle::Bare).unwrap(),
            Some(Field::Present(Value::Decimal(Decimal::new(1234, 2))))
        );
    }

    #[test]
    fn parquet_lookup_pruned_or_unknown_is_missing() {
        let rec = parquet(vec![Field::Present(Value::Int(1))], &["id"]);
        // A schema column dropped by projection pruning, and a name that
        // never existed, both resolve MISSING — never MissingHeader (that
        // code is CSV-header-specific; pruning must not error the stream).
        assert_eq!(
            column_value(&rec, "score", NameStyle::Bare).unwrap(),
            Some(Field::Missing)
        );
        assert_eq!(
            column_value(&rec, "nope", NameStyle::Bare).unwrap(),
            Some(Field::Missing)
        );
    }

    #[test]
    fn parquet_lookup_duplicate_names_ambiguous() {
        let rec = parquet(
            vec![Field::Present(Value::Int(1)), Field::Present(Value::Int(2))],
            &["a", "A"],
        );
        match column_value(&rec, "a", NameStyle::Bare) {
            Err(Error::Ambiguous(n)) => assert_eq!(n, "a"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
        // Quoted: exact case, no ambiguity.
        assert_eq!(
            column_value(&rec, "A", NameStyle::Quoted).unwrap(),
            Some(Field::Present(Value::Int(2)))
        );
    }

    // ------------------------------------------------------------------
    // Coverage round (2026-09-06): numeric / boolean / null edges not
    // reached by the main suite.
    // ------------------------------------------------------------------

    /// A one-column record whose value is an explicit `Value` (CSV fields
    /// are strings; these shim the numeric/bool/null typing directly).
    fn cell(v: Value) -> Record {
        Record::Csv(Columns::from_names(
            vec![Field::Present(v)],
            vec!["a".into()],
        ))
    }

    fn str_cell(s: &str) -> Record {
        cell(Value::String(s.to_string()))
    }

    #[test]
    fn unary_minus_value_shapes() {
        // Int negation stays Int (checked_neg).
        let rows = run("SELECT -s.a FROM S3Object s", vec![cell(Value::Int(-5))]);
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(5)));
        // i64::MIN overflows Int negation -> Decimal.
        let rows = run(
            "SELECT -s.a FROM S3Object s",
            vec![cell(Value::Int(i64::MIN))],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::from(9223372036854775808u64)))
        );
        // Decimal negation keeps the sign flip.
        let rows = run(
            "SELECT -s.a FROM S3Object s",
            vec![cell(Value::Decimal(Decimal::new(-25, 1)))],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::new(25, 1)))
        );
        // A bool is not a number -> value error. Only strings/raw numbers
        // parse lazily; bool/JSON never do.
        let mut engine = Engine::new(parse("SELECT -s.a FROM S3Object s").unwrap());
        match engine.next(cell(Value::Bool(true))) {
            Err(Error::Value(m)) => assert!(m.starts_with("invalid numeric value"), "{m}"),
            other => panic!("expected invalid numeric value, got {other:?}"),
        }
    }

    #[test]
    fn null_boolean_propagation() {
        // OR: true dominates; NULL-with-false is UNKNOWN, dropped.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a = 'x' OR NULL",
                vec![str_cell("x")]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a = 'x' OR NULL",
                vec![str_cell("y")]
            )
            .len(),
            0
        );
        // AND: false dominates even over NULL.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE false AND NULL",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
        // A non-boolean operand in boolean position is FALSE.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE 1 AND true",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
    }

    #[test]
    fn null_and_bool_comparisons() {
        // Null on either side is incomparable -> equality false, even NULL=NULL.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a = NULL",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE NULL = NULL",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
        // `!=` over an incomparable pair is false too (not flipped true).
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a != NULL",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
        // Booleans compare by value (the only ordering defined for them).
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE true = true",
                vec![str_cell("x")]
            )
            .len(),
            1
        );
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE true = false",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
    }

    #[test]
    fn in_list_null_element_is_unknown() {
        // A present match wins over a NULL element.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IN ('x', NULL)",
                vec![str_cell("x")]
            )
            .len(),
            1
        );
        // No match but a NULL element -> UNKNOWN (dropped), not false.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a IN ('y', NULL)",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
        // Negated NULL stays UNKNOWN (never flipped to a match).
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a NOT IN ('y', NULL)",
                vec![str_cell("x")]
            )
            .len(),
            0
        );
    }

    #[test]
    fn between_null_bound_is_unknown() {
        // A NULL bound makes the whole comparison incomparable -> UNKNOWN.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a BETWEEN NULL AND 5",
                vec![str_cell("3")]
            )
            .len(),
            0
        );
        // Comparable bounds match normally.
        assert_eq!(
            run(
                "SELECT * FROM S3Object s WHERE s.a BETWEEN 1 AND 5",
                vec![str_cell("3")]
            )
            .len(),
            1
        );
    }

    #[test]
    fn modulo_string_operand_and_zero() {
        // `%` parses a whole-number String operand via integral().
        let rows = run("SELECT s.a % 2 FROM S3Object s", vec![str_cell("6")]);
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(0)));
        // Modulo by zero is a value error, same as division.
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s WHERE s.a % 0 = 1").unwrap());
        match engine.next(str_cell("6")) {
            Err(Error::Value(m)) => assert_eq!(m, "division by zero"),
            other => panic!("expected division by zero, got {other:?}"),
        }
    }

    /// A two-column record (the `cell` helper is single-column).
    fn pair(a: Value, b: Value) -> Record {
        Record::Csv(Columns::from_names(
            vec![Field::Present(a), Field::Present(b)],
            vec!["a".into(), "b".into()],
        ))
    }

    #[test]
    fn int_arithmetic_fast_path() {
        // Int+Int stays Int for all three ops.
        let rows = run(
            "SELECT s.a + s.b FROM S3Object s",
            vec![pair(Value::Int(2), Value::Int(3))],
        );
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(5)));
        let rows = run(
            "SELECT s.a - s.b FROM S3Object s",
            vec![pair(Value::Int(5), Value::Int(3))],
        );
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(2)));
        let rows = run(
            "SELECT s.a * s.b FROM S3Object s",
            vec![pair(Value::Int(3), Value::Int(2))],
        );
        assert_eq!(rows[0].vals[0], Field::Present(Value::Int(6)));
        // i64::MIN - 1 overflows Int -> promotes to Decimal.
        let rows = run(
            "SELECT s.a - s.b FROM S3Object s",
            vec![pair(Value::Int(i64::MIN), Value::Int(1))],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::from(i64::MIN) - Decimal::from(1)))
        );
    }

    #[test]
    fn decimal_arithmetic() {
        // Decimal - and * stay Decimal.
        let rows = run(
            "SELECT s.a - 0.5 FROM S3Object s",
            vec![cell(Value::Decimal(Decimal::from(3)))],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::new(25, 1)))
        );
        let rows = run(
            "SELECT s.a * 0.5 FROM S3Object s",
            vec![cell(Value::Decimal(Decimal::from(3)))],
        );
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::new(15, 1)))
        );
    }

    #[test]
    fn unary_minus_string_parses() {
        // A String field negates via lazy numeric parse.
        let rows = run("SELECT -s.a FROM S3Object s", vec![str_cell("7")]);
        assert_eq!(
            rows[0].vals[0],
            Field::Present(Value::Decimal(Decimal::from(-7)))
        );
    }

    #[test]
    fn as_decimal_rejects_bool() {
        // A Bool operand in arithmetic is a value error (only comparisons
        // tolerate a non-numeric operand).
        let mut engine = Engine::new(parse("SELECT s.a + 1 FROM S3Object s").unwrap());
        match engine.next(cell(Value::Bool(true))) {
            Err(Error::Value(m)) => assert!(m.starts_with("invalid numeric value"), "{m}"),
            other => panic!("expected invalid numeric value, got {other:?}"),
        }
    }

    #[test]
    fn like_escape_non_string_rejected() {
        // A non-string ESCAPE operand (a numeric literal) is refused — the
        // `else` arm of the single-quoted-string match. Same split as
        // `like_escape_must_be_one_char`: the walk refuses it at request
        // level, the hand-built plan drives the runtime arm.
        assert_eq!(
            parse("SELECT * FROM S3Object s WHERE s.a LIKE 'h%' ESCAPE 5")
                .unwrap_err()
                .to_string(),
            "S3 select: ESCAPE must be a single character"
        );
        let mut engine = Engine::new(plan_with_where(like_expr(
            "a",
            false,
            "h%",
            Some(AstValue::Number("5".into(), false).into()),
        )));
        match engine.next(str_cell("hello")) {
            Err(Error::Value(m)) => assert_eq!(m, "ESCAPE must be a single character"),
            other => panic!("expected escape error, got {other:?}"),
        }
    }
}
