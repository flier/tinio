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
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName,
    ObjectNamePart, UnaryOperator,
};

use crate::SelectError;
use crate::row::{Field, Record, Value, display, parse_number};
use crate::sql::{Projection, QueryPlan};

/// One output row: keys (alias > plain field name > the record's names) and
/// the projected values (a filtered row never reaches here).
#[derive(Debug, Clone, PartialEq)]
pub struct OutRow {
    pub keys: Vec<String>,
    pub vals: Vec<Field>,
}

/// Plan-driven evaluator: WHERE filter, projection, LIMIT.
pub struct Engine {
    plan: QueryPlan,
    /// Rows emitted so far (the LIMIT counter).
    emitted: usize,
}

/// What an expression in flight sees: the record under test plus the FROM
/// alias (the JSON/parquet arms of `column_value` need it; CSV does not).
struct RowCtx<'a> {
    record: &'a Record,
    alias: &'a Option<String>,
}

impl Engine {
    pub fn new(plan: QueryPlan) -> Self {
        Self { plan, emitted: 0 }
    }

    /// One record through the plan. `Ok(None)` = filtered, or LIMIT reached.
    pub fn next(&mut self, rec: Record) -> Result<Option<OutRow>, SelectError> {
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

    /// The aggregate row, live in Task 7; end-of-stream for this task's
    /// non-aggregate plans.
    pub fn finish(&mut self) -> Result<Option<OutRow>, SelectError> {
        Ok(None)
    }

    /// One record's projection: `Wild` → the record's fields verbatim
    /// (its own names as keys); `Item` → the evaluated expression under the
    /// alias, else the plain field name, else the expression text.
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
                    // Task 9 gives JSON scalar-row semantics; empty until then.
                    Record::Json(_) => {}
                },
                Projection::Item { expr, alias } => {
                    vals.push(Field::Present(eval(expr, ctx)?));
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

/// The projection key of an expression without an alias.
fn plain_key(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .expect("compound identifier is non-empty")
            .value
            .clone(),
        other => other.to_string(),
    }
}

/// One column reference → the record's field at that name. CSV: `_N` maps
/// positionally; header names lookup case-insensitive unless `quoted`;
/// duplicate names → `Ambiguous`; a USE-mode name that matches no header →
/// `MissingHeader`, headerless modes fall through to MISSING. JSON/parquet
/// arms return `Ok(None)` (treated as MISSING) until Tasks 9/11.
fn column_value(
    rec: &Record,
    name: &str,
    alias: &Option<String>,
    quoted: bool,
) -> Result<Option<Field>, SelectError> {
    let _ = alias; // needed by the JSON/parquet arms (Tasks 9/11)
    match rec {
        Record::Csv(fields, names) => Ok(csv_field(fields, names, name, quoted)?),
        Record::Json(_) => Ok(None),
        Record::Parquet(_, _) => Ok(None),
    }
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
        Expr::CompoundIdentifier(parts) => {
            let last = parts.last().expect("compound identifier is non-empty");
            column_field(ctx, &last.value, last.quote_style.is_some())
        }
        // Path access is not meaningful on flat CSV; the full text never
        // matches a header → MISSING (Tasks 9/11 replace this arm).
        Expr::CompoundFieldAccess { .. } | Expr::JsonAccess { .. } => {
            column_field(ctx, &expr.to_string(), false)
        }
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
        Expr::Like { .. } => Err(SelectError::Unsupported("LIKE".into())),
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
    fn like_ilike_unsupported() {
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 LIKE 'a%'").unwrap());
        match engine.next(csv(&["a"], &["_1"])) {
            Err(SelectError::Unsupported(m)) => assert_eq!(m, "LIKE"),
            other => panic!("expected LIKE unsupported, got {other:?}"),
        }
        let mut engine =
            Engine::new(parse("SELECT * FROM S3Object s WHERE s._1 ILIKE 'a%'").unwrap());
        match engine.next(csv(&["a"], &["_1"])) {
            Err(SelectError::Unsupported(m)) => assert_eq!(m, "ILIKE"),
            other => panic!("expected ILIKE unsupported, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_function_arm_guards() {
        let mut engine = Engine::new(parse("SELECT count(*) FROM S3Object s").unwrap());
        match engine.next(csv(&["1"], &["_1"])) {
            Err(SelectError::Value(m)) => assert_eq!(m, "internal: unexpected aggregate call"),
            other => panic!("expected aggregate guard, got {other:?}"),
        }
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
}
