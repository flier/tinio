//! Engine: evaluator, WHERE / projection / LIMIT per record.
//!
//! `Engine` consumes one `Record` at a time; `next` filters by the plan's
//! WHERE (TRUE only), evaluates the projection, and caps emitted rows at
//! LIMIT. `finish` carries the aggregate row (Task 7) — for the
//! non-aggregate plans this task serves it returns `Ok(None)`.
//!
//! MISSING is a `Field` concept, not a `Value` one: `eval` collapses a
//! missing column to `Value::Null`, and the three MISSING-sensitive spots
//! (`IS NULL`[...], the `__s3_is_missing` sentinels) resolve their operand
//! at the `Field` level first so a present-but-null value stays distinct
//! from an absent field.

use std::cmp::Ordering;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sqlparser::ast::Value as AstValue;
use sqlparser::ast::{
    AccessExpr, BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Subscript, UnaryOperator,
};

use crate::SelectError;
use crate::json::to_value;
use crate::row::{Field, Record, Value, display, parse_number};
use crate::sql::{Projection, QueryPlan, contains_aggregate};

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
/// alias (the JSON/parquet arms of `column_value` need it; CSV does not).
struct RowCtx<'a> {
    record: &'a Record,
    alias: &'a Option<String>,
}

impl Engine {
    pub fn new(plan: QueryPlan) -> Self {
        let agg_state = plan.aggregates.then(|| AggState {
            count: 0,
            per_col: vec![AggCol::default(); plan.projections.len()],
        });
        Self {
            plan,
            emitted: 0,
            agg_state,
            agg_kinds: None,
        }
    }

    /// One record through the plan. `Ok(None)` = filtered, or LIMIT reached;
    /// in aggregate mode it always accumulates and returns `Ok(None)` (the
    /// single row is live in `finish`).
    pub fn next(&mut self, rec: Record) -> Result<Option<OutRow>, SelectError> {
        if self.agg_state.is_some() {
            return self.aggregate_next(rec);
        }
        if self.emitted >= self.plan.limit.unwrap_or(usize::MAX) {
            return Ok(None);
        }
        let ctx = RowCtx {
            record: &rec,
            alias: &self.plan.from.alias,
        };
        let passes = match &self.plan.where_expr {
            Some(expr) => eval(expr, &ctx)? == Value::Bool(true),
            None => true,
        };
        if !passes {
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
    pub fn finish(&mut self) -> Result<Option<OutRow>, SelectError> {
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
    fn aggregate_next(&mut self, rec: Record) -> Result<Option<OutRow>, SelectError> {
        self.classified()?;
        let ctx = RowCtx {
            record: &rec,
            alias: &self.plan.from.alias,
        };
        let passes = match &self.plan.where_expr {
            Some(expr) => eval(expr, &ctx)? == Value::Bool(true),
            None => true,
        };
        if passes {
            let kinds = self.agg_kinds.as_deref().expect("classified above");
            let state = self.agg_state.as_mut().expect("aggregate mode");
            accumulate(kinds, state, &ctx)?;
        }
        Ok(None)
    }

    /// Classify the projection list once (defense: the parse layer already
    /// guarantees every item is a bare aggregate call).
    fn classified(&mut self) -> Result<(), SelectError> {
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
    fn project(&self, ctx: &RowCtx) -> Result<OutRow, SelectError> {
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for item in &self.plan.projections {
            match item {
                Projection::Wild => match ctx.record {
                    Record::Csv(fields, names) | Record::Parquet(fields, names) => {
                        keys.extend(names.iter().cloned());
                        vals.extend(fields.iter().cloned());
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
                Projection::Item { expr, alias } => {
                    let field = match ctx.record {
                        Record::Json(_) => eval_field(expr, ctx)?,
                        _ => Field::Present(eval(expr, ctx)?),
                    };
                    vals.push(field);
                    keys.push(match alias {
                        Some(a) => a.clone(),
                        None => plain_key(expr),
                    });
                }
            }
        }
        Ok(OutRow { keys, vals })
    }
}

/// One passing record through every aggregate in the projection list.
fn accumulate(
    kinds: &[AggKind],
    state: &mut AggState,
    ctx: &RowCtx,
) -> Result<(), SelectError> {
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
fn present_value(expr: &Expr, ctx: &RowCtx) -> Result<Option<Value>, SelectError> {
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
) -> Result<(), SelectError> {
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
                    if is_min { text < current } else { text > current }
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
/// the string spine. A number that exceeds the 28-digit spine is a hard
/// error — the precision contract, never a silent string-column demotion
/// (a 29-digit "9…9" must not lexicographically lose to "2").
fn first_contrib_spine(v: &Value) -> Result<Option<Decimal>, SelectError> {
    match v {
        Value::Decimal(d) => Ok(Some(*d)),
        Value::Int(i) => Ok(Some(Decimal::from(*i))),
        Value::String(s) | Value::RawNumber(s) => {
            // Mirrors `parse_number`'s digit guard (row.rs) — the probe runs
            // before it so the precision error can be separated from the
            // plain non-numeric fall-through to string mode.
            if s.chars().filter(char::is_ascii_digit).count() > 28 {
                return Err(SelectError::Value(format!(
                    "numeric value exceeds 28-digit precision: {s}"
                )));
            }
            Ok(parse_number(s).ok())
        }
        _ => Ok(None),
    }
}

/// One aggregate column's finished value: COUNT → Int; SUM/AVG/MIN/MAX with
/// zero contributing values → Null; AVG → Decimal scale 10.
fn finish_value(kind: &AggKind, state: &AggState, col: &AggCol) -> Result<Value, SelectError> {
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
fn aggregate_kind(item: &Projection) -> Result<AggKind, SelectError> {
    let not_aggregate = || {
        SelectError::Parse("non-aggregate expression in aggregate select list".into())
    };
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
        return Err(SelectError::Parse("distinct not supported".into()));
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
fn precision_overflow() -> SelectError {
    SelectError::Value("numeric value exceeds 28-digit precision".into())
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
fn column_value(
    rec: &Record,
    name: &str,
    alias: &Option<String>,
    quoted: bool,
) -> Result<Option<Field>, SelectError> {
    let _ = alias; // unused by flat-record lookups (JSON paths use it)
    match rec {
        Record::Csv(fields, names) => Ok(csv_field(fields, names, name, quoted)?),
        Record::Json(v) => Ok(Some(json_column(v.as_ref(), name, quoted)?)),
        Record::Parquet(fields, names) => parquet_field(fields, names, name, quoted),
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
    quoted: bool,
) -> Result<Field, SelectError> {
    let Some(v) = value else { return Ok(Field::Missing) };
    match v {
        serde_json::Value::Object(map) => match json_lookup(map, name, quoted)? {
            None if name == "_1" => Ok(Field::Present(to_value(v))),
            Some(field) => Ok(Field::Present(to_value(field))),
            None => Ok(Field::Missing),
        },
        _ => Ok(Field::Present(to_value(v))),
    }
}

/// Object key lookup: case-insensitive when unquoted, exact when quoted;
/// a case-folded duplicate is ambiguous (AWS: two attrs differing only in
/// case → `AmbiguousFieldName`).
fn json_lookup<'a>(
    map: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
    quoted: bool,
) -> Result<Option<&'a serde_json::Value>, SelectError> {
    let mut found: Vec<&serde_json::Value> = Vec::new();
    for (key, value) in map {
        let matches = if quoted {
            key == name
        } else {
            key.eq_ignore_ascii_case(name)
        };
        if matches {
            found.push(value);
        }
    }
    match found.as_slice() {
        [] => Ok(None),
        [value] => Ok(Some(*value)),
        _ => Err(SelectError::Ambiguous(name.into())),
    }
}

/// A JSON compound identifier path (`_1.dir_name`, `s.a.b`): the first part
/// resolves on the record (`_1`/the FROM alias → the row; any other name →
/// a field), the remaining parts walk object fields.
fn json_path(
    record: Option<&serde_json::Value>,
    parts: &[Ident],
    alias: &Option<String>,
) -> Result<Field, SelectError> {
    let (first, rest) = parts.split_first().expect("compound identifier is non-empty");
    let mut field = match first.value.as_str() {
        "_1" => json_row_ref(record, first.quote_style.is_some())?,
        name if alias.as_deref() == Some(name) => json_row_value(record),
        name => json_column(record, name, first.quote_style.is_some())?,
    };
    for part in rest {
        field = json_dot_step(field, &part.value, part.quote_style.is_some())?;
    }
    Ok(field)
}

/// The row itself under `_1` — the whole record value unless the object
/// carries a key matching `_1` (field wins).
fn json_row_ref(record: Option<&serde_json::Value>, quoted: bool) -> Result<Field, SelectError> {
    let Some(v) = record else { return Ok(Field::Missing) };
    if let serde_json::Value::Object(map) = v
        && let Some(field) = json_lookup(map, "_1", quoted)?
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

/// One dot step: an object key lookup (CI/exact per the part's quote);
/// anything else is MISSING.
fn json_dot_step(field: Field, name: &str, quoted: bool) -> Result<Field, SelectError> {
    match field {
        Field::Missing => Ok(Field::Missing),
        Field::Present(Value::Json(j)) => json_dot(j, name, quoted),
        Field::Present(_) => Ok(Field::Missing),
    }
}

/// One dot look up on a boxed JSON value.
fn json_dot(j: Box<serde_json::Value>, name: &str, quoted: bool) -> Result<Field, SelectError> {
    let serde_json::Value::Object(map) = j.as_ref() else {
        return Ok(Field::Missing);
    };
    match json_lookup(map, name, quoted)? {
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
) -> Result<Field, SelectError> {
    let mut field = match root {
        Expr::Identifier(id) if id.value == "_1" => json_row_ref(record, id.quote_style.is_some())?,
        Expr::Identifier(id) if alias.as_deref() == Some(id.value.as_str()) => {
            json_row_value(record)
        }
        Expr::Identifier(id) => json_column(record, &id.value, id.quote_style.is_some())?,
        // A non-identifier root has no path into the record.
        _ => Field::Missing,
    };
    for step in chain {
        field = json_step(field, step)?;
    }
    Ok(field)
}

/// One access-chain step.
fn json_step(field: Field, step: &AccessExpr) -> Result<Field, SelectError> {
    match (field, step) {
        (Field::Missing, _) => Ok(Field::Missing),
        (Field::Present(Value::Json(j)), AccessExpr::Dot(Expr::Identifier(id))) => {
            json_dot(j, &id.value, id.quote_style.is_some())
        }
        (Field::Present(Value::Json(_)), AccessExpr::Dot(_)) => Ok(Field::Missing),
        (Field::Present(Value::Json(j)), AccessExpr::Subscript(s)) => json_subscript(j, s),
        (Field::Present(_), _) => Ok(Field::Missing),
    }
}

/// One bracket subscript: a literal non-negative integer indexes an array
/// (out-of-range / not-an-array → MISSING); a literal string names an
/// object key exactly; anything else → MISSING.
fn json_subscript(j: Box<serde_json::Value>, s: &Subscript) -> Result<Field, SelectError> {
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
    quoted: bool,
) -> Result<Option<Field>, SelectError> {
    // `_N` positional notation wins over headers.
    if let Some(index) = positional(name) {
        return Ok(Some(fields.get(index).cloned().unwrap_or(Field::Missing)));
    }
    // Header lookup: exact-case when quoted, case-insensitive otherwise.
    // A duplicate (case-folded for unquoted, literal for quoted) is
    // ambiguous; the later index is kept for the lookup candidate.
    let mut seen: Vec<usize> = Vec::new();
    for (i, header) in names.iter().enumerate() {
        let matches = if quoted {
            header == name
        } else {
            header.eq_ignore_ascii_case(name)
        };
        if matches {
            seen.push(i);
        }
    }
    match seen.as_slice() {
        [index] => Ok(fields
            .get(*index)
            .cloned()
            .map_or(Some(Field::Missing), Some)),
        [] => {
            // USE-mode headers are the record's names; a named ref matching
            // none of them is a missing-header error. Headerless modes
            // (names are the positional `_1..` alias set) fall through to
            // MISSING.
            if default_names(names) {
                Ok(Some(Field::Missing))
            } else {
                Err(SelectError::MissingHeader(name.into()))
            }
        }
        _ => Err(SelectError::Ambiguous(name.into())),
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
    quoted: bool,
) -> Result<Option<Field>, SelectError> {
    match csv_field(fields, names, name, quoted) {
        Ok(field) => Ok(field),
        Err(SelectError::MissingHeader(_)) => Ok(Some(Field::Missing)),
        Err(e) => Err(e),
    }
}

/// `_N` → zero-based index; `_0`/`_`/`_a` are not positional.
fn positional(name: &str) -> Option<usize> {
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
fn default_names(names: &[String]) -> bool {
    names
        .iter()
        .enumerate()
        .all(|(i, n)| n == &format!("_{}", i + 1))
}

/// Missing-aware resolution of an operand: column references go through
/// `column_value` (an absent field stays `Field::Missing`); any other
/// expression produces a value and is never MISSING.
fn eval_field(expr: &Expr, ctx: &RowCtx) -> Result<Field, SelectError> {
    match expr {
        Expr::Identifier(id) => column_field(ctx, &id.value, id.quote_style.is_some()),
        Expr::CompoundIdentifier(parts) => match ctx.record {
            Record::Json(v) => json_path(v.as_ref(), parts, ctx.alias),
            _ => {
                let last = parts.last().expect("compound identifier is non-empty");
                column_field(ctx, &last.value, last.quote_style.is_some())
            }
        },
        // Path access on flat CSV is not meaningful — the full text never
        // matches a header → MISSING; JSON resolves the real access chain.
        Expr::CompoundFieldAccess { root, access_chain } => match ctx.record {
            Record::Json(v) => json_access(v.as_ref(), root, access_chain, ctx.alias),
            _ => column_field(ctx, &expr.to_string(), false),
        },
        Expr::JsonAccess { .. } => column_field(ctx, &expr.to_string(), false),
        // `(x) IS NULL` ≡ `x IS NULL`: parenthesized columns stay MISSING
        // rather than collapsing through `eval` into a present NULL.
        Expr::Nested(e) => eval_field(e, ctx),
        other => Ok(Field::Present(eval(other, ctx)?)),
    }
}

fn column_field(ctx: &RowCtx, name: &str, quoted: bool) -> Result<Field, SelectError> {
    Ok(column_value(ctx.record, name, ctx.alias, quoted)?.unwrap_or(Field::Missing))
}

/// The single expression argument Task 3's `IS [NOT] MISSING` rewrite emits.
fn sentinel_operand(f: &Function) -> Result<Expr, SelectError> {
    match &f.args {
        FunctionArguments::List(list) => match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => Ok(e.clone()),
            _ => Err(SelectError::Value(
                "internal: unexpected aggregate call".into(),
            )),
        },
        _ => Err(SelectError::Value(
            "internal: unexpected aggregate call".into(),
        )),
    }
}

/// The last identifier of a function name — `__s3_is_missing`.
fn last_name(name: &ObjectName) -> String {
    match name.0.last() {
        Some(ObjectNamePart::Identifier(i)) => i.value.clone(),
        _ => String::new(),
    }
}

/// Evaluate a parsed expression tree to a scalar. `eval` runs no code of
/// its own — the `Expr` is sqlparser's AST and every arithmetic/comparison
/// rule below is data-driven; a missing column collapses to `Value::Null`
/// here (`eval_field` is the MISSING-aware path).
fn eval(expr: &Expr, ctx: &RowCtx) -> Result<Value, SelectError> {
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
        Expr::UnaryOp { op, expr } => match op {
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
            other => Err(SelectError::Unsupported(other.to_string())),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = eval(left, ctx)?;
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
                other => Err(SelectError::Unsupported(other.to_string())),
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
        Expr::IsNull(e) => Ok(Value::Bool(matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Null)
        ))),
        Expr::IsNotNull(e) => Ok(Value::Bool(!matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Null)
        ))),
        Expr::IsTrue(e) => Ok(Value::Bool(matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Bool(true))
        ))),
        Expr::IsNotTrue(e) => Ok(Value::Bool(!matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Bool(true))
        ))),
        Expr::IsFalse(e) => Ok(Value::Bool(matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Bool(false))
        ))),
        Expr::IsNotFalse(e) => Ok(Value::Bool(!matches!(
            eval_field(e, ctx)?,
            Field::Present(Value::Bool(false))
        ))),
        Expr::Function(f) => {
            // Task 3's `IS [NOT] MISSING` rewrite (sqlparser 0.57 has no
            // IsMissing variant) is the only function form over a
            // non-aggregate plan; anything else is an aggregate guard.
            if last_name(&f.name).eq_ignore_ascii_case("__s3_is_missing") {
                let operand = sentinel_operand(f)?;
                return Ok(Value::Bool(matches!(
                    eval_field(&operand, ctx)?,
                    Field::Missing
                )));
            }
            if last_name(&f.name).eq_ignore_ascii_case("__s3_is_not_missing") {
                let operand = sentinel_operand(f)?;
                return Ok(Value::Bool(!matches!(
                    eval_field(&operand, ctx)?,
                    Field::Missing
                )));
            }
            Err(SelectError::Value(
                "internal: unexpected aggregate call".into(),
            ))
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => eval_like(expr, pattern, escape_char, *any, *negated, ctx),
        Expr::ILike { .. } => Err(SelectError::Unsupported("ILIKE".into())),
        other => Err(SelectError::Unsupported(other.to_string())),
    }
}

/// SQL literals: numbers → `Int` when they fit, else `Decimal`.
fn literal(v: &AstValue) -> Result<Value, SelectError> {
    match v {
        AstValue::Number(n, _) => match n.parse::<i64>() {
            Ok(i) => Ok(Value::Int(i)),
            Err(_) => parse_number(n).map(Value::Decimal),
        },
        AstValue::SingleQuotedString(s) => Ok(Value::String(s.clone())),
        AstValue::Boolean(b) => Ok(Value::Bool(*b)),
        AstValue::Null => Ok(Value::Null),
        other => Err(SelectError::Unsupported(other.to_string())),
    }
}

/// Unary minus: numeric negation with Int overflow promoted to Decimal.
fn unary_minus(v: Value) -> Result<Value, SelectError> {
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
fn eval_in(expr: &Expr, list: &[Expr], negated: bool, ctx: &RowCtx) -> Result<Value, SelectError> {
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
) -> Result<Value, SelectError> {
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
    escape_char: &Option<String>,
    any: bool,
    negated: bool,
    ctx: &RowCtx,
) -> Result<Value, SelectError> {
    // Snowflake's `LIKE ANY` is outside the AWS surface and the covered
    // grammar: refuse rather than silently run plain LIKE semantics.
    if any {
        return Err(SelectError::Unsupported("LIKE ANY".into()));
    }
    let escape = like_escape(escape_char)?;
    let subject = eval(expr, ctx)?;
    let pattern = eval(pattern, ctx)?;
    let matched = match (&subject, &pattern) {
        (Value::String(_), Value::String(_)) => {
            like_match(&display(&subject), &display(&pattern), escape)
        }
        _ => false,
    };
    Ok(Value::Bool(if negated { !matched } else { matched }))
}

/// The ESCAPE operand: exactly one character. sqlparser 0.57 parses any
/// literal string here without validating the width, so a bad one is a
/// value error.
fn like_escape(v: &Option<String>) -> Result<Option<char>, SelectError> {
    match v {
        None => Ok(None),
        Some(s) => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(Some(c)),
                _ => Err(SelectError::Value(
                    "ESCAPE must be a single character".into(),
                )),
            }
        }
    }
}

/// O(n·m) wildcard DP — no backtracking, so `%`-heavy patterns stay
/// polynomial under the expression and record caps (review 2026-09-05 #5).
fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let text: Vec<char> = text.chars().collect();
    let toks = like_tokens(pattern, escape);
    let m = text.len();
    let mut prev = vec![false; m + 1];
    let mut cur = vec![false; m + 1];
    prev[0] = true;
    for tok in &toks {
        // `%` may match the empty prefix; nothing else may.
        cur[0] = matches!(tok, LikeTok::Star) && prev[0];
        for j in 1..=m {
            cur[j] = match tok {
                LikeTok::Star => prev[j] || cur[j - 1],
                LikeTok::Single => prev[j - 1],
                LikeTok::Char(c) => prev[j - 1] && text[j - 1] == *c,
            };
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[m]
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
fn arith(op: &BinaryOperator, a: Value, b: Value) -> Result<Value, SelectError> {
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
fn as_decimal(v: &Value) -> Result<Decimal, SelectError> {
    match v {
        Value::Decimal(d) => Ok(*d),
        Value::Int(i) => Ok(Decimal::from(*i)),
        Value::String(s) | Value::RawNumber(s) => parse_number(s),
        // Null propagates before arithmetic reaches here.
        Value::Null => Ok(Decimal::ZERO),
        Value::Bool(_) | Value::Json(_) => Err(not_numeric(v)),
    }
}

fn not_numeric(v: &Value) -> SelectError {
    SelectError::Value(format!("invalid numeric value: {}", display(v)))
}

/// Decimal arithmetic: strings/raw numbers parse here (strict — only
/// comparisons are tolerant of parse failure).
fn decimal_arith(op: &BinaryOperator, x: Decimal, y: Decimal) -> Result<Value, SelectError> {
    let overflow = || SelectError::Value("numeric value exceeds 28-digit precision".into());
    match op {
        BinaryOperator::Plus => x.checked_add(y).map(Value::Decimal).ok_or_else(overflow),
        BinaryOperator::Minus => x.checked_sub(y).map(Value::Decimal).ok_or_else(overflow),
        BinaryOperator::Multiply => x.checked_mul(y).map(Value::Decimal).ok_or_else(overflow),
        BinaryOperator::Divide => {
            if y.is_zero() {
                return Err(SelectError::Value("division by zero".into()));
            }
            x.checked_div(y)
                .map(|d| Value::Decimal(d.round_dp(10)))
                .ok_or_else(overflow)
        }
        BinaryOperator::Modulo => modulo(&Value::Decimal(x), &Value::Decimal(y)),
        other => Err(SelectError::Unsupported(other.to_string())),
    }
}

/// `%` is integral-only: operands must be whole numbers; the result is
/// Int (checked; the MIN % -1 corner promotes to a Decimal remainder).
fn modulo(a: &Value, b: &Value) -> Result<Value, SelectError> {
    let x = integral(a).ok_or_else(|| not_numeric(a))?;
    let y = integral(b).ok_or_else(|| not_numeric(b))?;
    if y == 0 {
        return Err(SelectError::Value("division by zero".into()));
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

    use crate::row::Value;
    use crate::sql::parse;

    use super::*;

    fn csv(fields: &[&str], names: &[&str]) -> Record {
        Record::Csv(
            fields
                .iter()
                .map(|f| Field::Present(Value::String(f.to_string())))
                .collect(),
            names.iter().map(|n| n.to_string()).collect(),
        )
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
            Record::Csv(
                vec![Field::Present(Value::Null), Field::Missing],
                vec!["a".into(), "b".into()],
            )
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
        let rec = Record::Csv(vec![Field::Present(Value::Null)], vec!["a".into()]);
        let rows = run("SELECT * FROM S3Object s WHERE s.a IS NULL", vec![rec]);
        assert_eq!(rows.len(), 1);
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
        let t = Record::Csv(vec![Field::Present(Value::Bool(true))], vec!["a".into()]);
        let f = Record::Csv(vec![Field::Present(Value::Bool(false))], vec!["a".into()]);
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
        // sqlparser 0.57 parses any literal string after ESCAPE; standard
        // SQL takes exactly one character.
        let mut engine = Engine::new(
            parse("SELECT * FROM S3Object s WHERE s._1 LIKE 'a\\%b' ESCAPE '\\%'").unwrap(),
        );
        match engine.next(csv(&["a%b"], &["_1"])) {
            Err(SelectError::Value(m)) => assert_eq!(m, "ESCAPE must be a single character"),
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
        // Numbers, bools and NULL never match — no coercion.
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
        assert!(like_match(
            &format!("{}{}", "b".repeat(300), "a".repeat(300)),
            &pattern,
            None
        ));
        // The trailing `b`s cannot be absorbed: no star follows the last
        // literal `a`.
        assert!(!like_match(
            &format!("{}{}", "a".repeat(300), "b".repeat(300)),
            &pattern,
            None
        ));
    }

    #[test]
    fn like_any_is_refused() {
        // Snowflake's `LIKE ANY` is outside the AWS surface: refuse rather
        // than silently run single-pattern LIKE semantics.
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 LIKE ANY 'x%'").unwrap());
        match engine.next(csv(&["x"], &["_1"])) {
            Err(SelectError::Unsupported(m)) => assert_eq!(m, "LIKE ANY"),
            other => panic!("expected LIKE ANY unsupported, got {other:?}"),
        }
    }

    #[test]
    fn ilike_is_unsupported() {
        // AWS S3 Select has no case-insensitive LIKE: reject rather than
        // silently behave as a case-sensitive LIKE (review 2026-09-05b).
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 ILIKE 'X'").unwrap());
        match engine.next(csv(&["x"], &["_1"])) {
            Err(SelectError::Unsupported(m)) => assert_eq!(m, "ILIKE"),
            other => panic!("expected ILIKE unsupported, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_arm_guards() {
        // A non-aggregate plan never carries an aggregate call (sql.rs sets
        // `aggregates` only for the five names); any other function is an
        // unreachable shape — guard, not a real result.
        let mut engine = Engine::new(parse("SELECT unknown_fn(s._1) FROM S3Object s").unwrap());
        match engine.next(csv(&["1"], &["_1"])) {
            Err(SelectError::Value(m)) => assert_eq!(m, "internal: unexpected aggregate call"),
            other => panic!("expected aggregate guard, got {other:?}"),
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
        let rec = Record::Csv(vec![Field::Present(Value::Null)], vec!["a".into()]);
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
        assert_eq!(engine.next(csv(&["1", "x", "10"], &["_1", "_2", "_3"])).unwrap(), None);
        match engine.next(csv(&["2", "y", "x"], &["_1", "_2", "_3"])) {
            Err(SelectError::Value(m)) => assert_eq!(m, "invalid numeric value: x"),
            other => panic!("expected invalid numeric error, got {other:?}"),
        }
    }

    #[test]
    fn avg_is_decimal_scale_ten() {
        // Exact average: 1.5.
        let row = run_agg(
            "SELECT avg(s._1) FROM S3Object s",
            vec![
                csv(&["1"], &["_1"]),
                csv(&["2"], &["_1"]),
            ],
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
            vec![Field::Present(Value::Decimal(Decimal::new(16666666667, 10)))]
        );
    }

    #[test]
    fn min_max_numeric_strings_are_decimals() {
        // First contributor parses → the column is numeric: extrema are
        // parsed Decimals, so ['10','2'] → min 2, max 10 (not "10").
        let row = run_agg(
            "SELECT min(s._1), max(s._1) FROM S3Object s",
            vec![
                csv(&["10"], &["_1"]),
                csv(&["2"], &["_1"]),
            ],
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
            Err(SelectError::Value(m)) => assert_eq!(m, "invalid numeric value: x"),
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
            Err(SelectError::Value(m)) => {
                assert_eq!(m, format!("numeric value exceeds 28-digit precision: {big}"))
            }
            other => panic!("expected precision error, got {other:?}"),
        }
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
        let rec = Record::Csv(
            vec![Field::Present(Value::Null), Field::Present(Value::Null)],
            vec!["a".into(), "b".into()],
        );
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
            vec!["count(*)", "count(s._1)", "sum(s._3)", "avg(s._3)", "min(s._2)"]
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
            Err(SelectError::Value(m)) => assert_eq!(m, "division by zero"),
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
            Err(SelectError::Value(m)) => assert_eq!(m, "invalid numeric value: 5.5"),
            other => panic!("expected integral-only modulo error, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_on_non_numeric_is_an_error() {
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 + 1 > 10").unwrap());
        match engine.next(csv(&["abc"], &["_1"])) {
            Err(SelectError::Value(m)) => assert_eq!(m, "invalid numeric value: abc"),
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
            Err(SelectError::MissingHeader(m)) => assert_eq!(m, "nope"),
            other => panic!("expected missing header, got {other:?}"),
        }
    }

    #[test]
    fn case_insensitive_duplicate_header_is_ambiguous() {
        let mut engine = Engine::new(parse("SELECT * FROM S3Object s WHERE s.x = '1'").unwrap());
        match engine.next(csv(&["1"], &["X", "x"])) {
            Err(SelectError::Ambiguous(m)) => assert_eq!(m, "x"),
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
        Record::Parquet(fields, names.iter().map(|n| n.to_string()).collect())
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
            column_value(&rec, "ID", &None, false).unwrap(),
            Some(Field::Present(Value::Int(1)))
        );
        assert_eq!(
            column_value(&rec, "price", &None, false).unwrap(),
            Some(Field::Present(Value::Decimal(Decimal::new(1234, 2))))
        );
        // `_N` positional spine works like CSV.
        assert_eq!(
            column_value(&rec, "_2", &None, false).unwrap(),
            Some(Field::Present(Value::Decimal(Decimal::new(1234, 2))))
        );
    }

    #[test]
    fn parquet_lookup_pruned_or_unknown_is_missing() {
        let rec = parquet(
            vec![Field::Present(Value::Int(1))],
            &["id"],
        );
        // A schema column dropped by projection pruning, and a name that
        // never existed, both resolve MISSING — never MissingHeader (that
        // code is CSV-header-specific; pruning must not error the stream).
        assert_eq!(
            column_value(&rec, "score", &None, false).unwrap(),
            Some(Field::Missing)
        );
        assert_eq!(
            column_value(&rec, "nope", &None, false).unwrap(),
            Some(Field::Missing)
        );
    }

    #[test]
    fn parquet_lookup_duplicate_names_ambiguous() {
        let rec = parquet(
            vec![Field::Present(Value::Int(1)), Field::Present(Value::Int(2))],
            &["a", "A"],
        );
        match column_value(&rec, "a", &None, false) {
            Err(SelectError::Ambiguous(n)) => assert_eq!(n, "a"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
        // Quoted: exact case, no ambiguity.
        assert_eq!(
            column_value(&rec, "A", &None, true).unwrap(),
            Some(Field::Present(Value::Int(2)))
        );
    }
}
