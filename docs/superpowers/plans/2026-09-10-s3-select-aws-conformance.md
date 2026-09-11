# S3 Select AWS Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the S3 Select acceptance surface so no query parses and then silently does less than it says, replace the misleading unknown-function failure with a request-level 400, and pin the parquet type table to the AWS list.

**Architecture:** Three checks are added to the existing validator in `sql.rs`, all shared by both FROM paths (the custom-path skeleton and the stock parser). The definition of "which expression forms the evaluator supports" — and the function allowlist that belongs to it — moves into one predicate in `engine.rs`, consulted by both `eval`'s catch-all arms and the validator's eager walk, so the parse-time refusal and the runtime error cannot drift. `parquet.rs` gains two arrow date arms.

**Tech Stack:** Rust, `sqlparser` 0.62 (pinned; `visitor` feature enabled), `arrow`/`parquet` 59 (optional, behind the `parquet` feature), `time` 0.3 with the `formatting` feature.

**Spec:** `docs/superpowers/specs/2026-09-10-s3-select-aws-conformance-design.md`

## Global Constraints

- **Git: never commit without the user's explicit approval, per operation.** The repo's `CLAUDE.md` overrides the plan template: leave changes in the tree, report them pending, and ask before every `git add`/`git commit`. No agent/AI trailers in commit messages — no `Co-Authored-By:`, no "Generated with".
- **English only** in code, comments, docs, and commit messages.
- Test commands: `cargo test -p tinio-select` for everything except Task 5, which additionally needs `cargo test -p tinio-select --features parquet` (its tests are behind `#[cfg(feature = "parquet")]` and silently do not compile otherwise).
- **Baseline: `cargo test -p tinio-select` → 284 passed + 2 doctests, 0 failed.** Each task's expected result is "PASS, 0 failed, and the baseline count must not shrink" — a rewrite may move a test between modules but must never delete one.
- Do not touch `path.rs`, the path grammar, `row.rs`, `output.rs`, `record.rs`, `events.rs`, `error.rs`, `tinio-server`, or `traceability.rs`. `json.rs` may be modified **in its `#[cfg(test)]` module only** (the A2 pin, Task 6). **(breached 2026-09-11)** `dialect.rs` was touched after all — exactly one method, the `Dialect::dialect()` identity override closing `B'…'`/`R'…'`; see the follow-up note at the end of this plan.
- `error.rs` gains **no** new variant: every refusal is `Error::Parse` or `Error::Unsupported`.
- Error messages use the existing channels — `Error::Unsupported("<NAME>")` for a clause or function we decline, `Error::Parse(...)` for a malformed query.

## Facts established before planning (do not re-derive)

- `Expr::Wildcard` **cannot** reach the visitor with any expression content: `count(*)`'s `*` is `FunctionArgExpr::Wildcard`, and `SELECT *` is `SelectItem::Wildcard(WildcardAdditionalOptions)`, which holds no `Expr`. In 0.62 `Expr::Wildcard` carries an `AttachedToken` payload whose generated visit arm recurses only into no-op leaf tokens — no expression is ever visited. `pre_visit_expr` fires exactly once for `SELECT count(*) FROM S3Object s` (the `Function` node) and zero times for `SELECT * FROM S3Object s`. **No `Expr::Wildcard` arm is needed.**
- `is_aggregate_name` takes `&ObjectName`, not `&str` (`sql.rs:367`). `single_part_name(&ObjectName) -> Option<String>` already exists (`engine.rs:479`). `is_sentinel_call` is already `pub(crate)` (`engine.rs:873`).
- `SentinelNames::mint()` is `pub` (`sql.rs:56`), `FromClause { segments, alias }` and `QueryPlan`'s six fields are all `pub` — a hand-built `QueryPlan` is constructible in `engine.rs`'s test module.
- `select.feature` has 34 scenarios and exactly one **feature-level** `@FR-034` tag; no scenario carries a tag. A new scenario needs no tag.

---

### Task 1: The shared support predicate in `engine.rs`

The single definition of "which expression forms the evaluator supports", plus the function allowlist it consults. `eval`'s catch-all arms and (in Task 4) the validator's walk both use it.

**Files:**
- Modify: `crates/tinio-select/src/engine.rs` (add the enum, the allowlist and the predicate above `fn eval` at `engine.rs:884`; change six arms in `eval`; rewrite three tests)
- Modify: `crates/tinio-select/src/sql.rs:367` (`is_aggregate_name` → `pub(crate)`, signature unchanged)
- Test: `crates/tinio-select/src/engine.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Consumes: `is_sentinel_call(&ObjectName, &str) -> bool` (`engine.rs:873`, already `pub(crate)`); `single_part_name(&ObjectName) -> Option<String>` (`engine.rs:479`); `is_aggregate_name(&ObjectName) -> bool` (made `pub(crate)`).
- Produces: `pub(crate) enum UnsupportedForm { Expression(Cow<'static, str>), Function(String) }` implementing `Display`; `pub(crate) fn unsupported_form(expr: &Expr, missing_name: &str) -> Option<UnsupportedForm>`; `fn is_allowed_function(name: &ObjectName) -> bool` (private to `engine.rs`). Task 4 calls the predicate and nothing else.

- [ ] **Step 1: Write the failing tests**

Append to `engine.rs`'s test module:

```rust
/// Builds `Expr::Function` with a single-part name — the shape the parser
/// produces for `LOWER(x)`, `count(x)` and the request sentinel alike.
fn func(name: &str) -> Expr {
    Expr::Function(sqlparser::ast::Function {
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

/// One row per form the walk can meet: the query text, and the exact
/// `Display` of the predicate's verdict. This is the corpus that replaces
/// the earlier empty stub — a table-driven test, not a placeholder.
#[test]
fn unsupported_form_corpus() {
    let cases: &[(&str, &str)] = &[
        ("SELECT CASE WHEN s.a > 1 THEN 1 ELSE 0 END FROM S3Object s",
         "unsupported expression: CASE"),
        ("SELECT CAST(s.a AS INT) FROM S3Object s", "unsupported expression: CAST"),
        // SUBSTRING goes in the projection; ILIKE / LIKE ANY are infix-only
        // and must live in the WHERE clause.
        ("SELECT SUBSTRING(s.a, 1) FROM S3Object s",
         "unsupported expression: SUBSTRING"),
        ("SELECT s.a FROM S3Object s WHERE s.a ILIKE 'X'",
         "unsupported expression: ILIKE"),
        ("SELECT s.a FROM S3Object s WHERE s._1 LIKE ANY 'x%'",
         "unsupported expression: LIKE ANY"),
        ("SELECT LOWER(s.a) FROM S3Object s", "unsupported function: LOWER"),
        ("SELECT COALESCE(s.a, 'x') FROM S3Object s", "unsupported function: COALESCE"),
    ];
    for (sql, expected) in cases {
        let plan = crate::sql::parse(sql).expect("parses; rejection is the walk's job");
        let expr = form_expr(&plan);
        // is_missing() returns an owned String — borrow it here.
        match unsupported_form(expr, &plan.missing.is_missing()) {
            Some(form) => assert_eq!(form.to_string(), *expected, "for {sql}"),
            None => panic!("expected a verdict for {sql}"),
        }
    }
}

#[test]
fn unsupported_form_diagnostics_have_the_two_shapes() {
    assert_eq!(
        UnsupportedForm::Expression(Cow::Borrowed("CASE")).to_string(),
        "unsupported expression: CASE"
    );
    assert_eq!(
        UnsupportedForm::Function("LOWER".into()).to_string(),
        "unsupported function: LOWER"
    );
}
```

The corpus test's `form_expr` is a small local helper you add in this step — it must return the node that carries the form, which for infix-only forms is the WHERE clause, not the first projection:

```rust
/// The expression carrying the form under test: the WHERE clause when the
/// query has one (infix-only forms like ILIKE and LIKE ANY live there),
/// otherwise the first projection expression.
fn form_expr(plan: &QueryPlan) -> &Expr {
    if let Some(where_expr) = &plan.where_expr {
        return where_expr;
    }
    match plan.projections.first().expect("a projection") {
        Projection::Item { expr, .. } => expr,
        Projection::Wild => panic!("corpus cases are not `SELECT *`"),
    }
}
```

This step's test code also uses `FunctionArgumentList`, `CastKind` and `DataType`, which the engine test module does not import today — add them (Step 5 will additionally need `FromClause` and `SentinelNames` from `crate::sql`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-select unsupported_form -v`
Expected: FAIL — `cannot find function unsupported_form`, `cannot find type UnsupportedForm`.

- [ ] **Step 3: Add the enum, the allowlist and the predicate**

In `engine.rs`, immediately above `fn eval`:

```rust
/// Why the evaluator cannot run an expression form. The single definition of
/// what the engine supports: `eval`'s catch-all arms and the validator's
/// parse-time walk both consult [`unsupported_form`], so the request-level
/// refusal and the runtime error cannot drift apart.
///
/// `Expression` borrows when the form has a fixed name and owns the rendered
/// text otherwise, so any variant not named below keeps exactly the
/// diagnostic it produced before the predicate existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnsupportedForm {
    Expression(Cow<'static, str>),
    Function(String),
}

impl std::fmt::Display for UnsupportedForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expression(name) => write!(f, "unsupported expression: {name}"),
            Self::Function(name) => write!(f, "unsupported function: {name}"),
        }
    }
}

/// The function names the engine evaluates. In this spec it is exactly the
/// aggregate set; the scalar-function spec extends this table alone.
///
/// It is deliberately a different function from `contains_aggregate`'s
/// predicate even while they agree: `is_aggregate_name` decides whether a
/// query is in aggregate mode (governing `*` and the scan-accumulate path),
/// so growing it with scalar names would push `SELECT LOWER(s.a) …` down the
/// wrong execution path.
fn is_allowed_function(name: &ObjectName) -> bool {
    is_aggregate_name(name)
}

/// Classify one node. `None` means the engine evaluates this form; `Some`
/// names the reason it cannot. Switches on the `Expr` *variant* only — an
/// identifier whose text happens to be a keyword (`s.CAST`) is supported,
/// which is the recorded AWS divergence (spec Q2/Q8).
pub(crate) fn unsupported_form(expr: &Expr, missing_name: &str) -> Option<UnsupportedForm> {
    // Forms `eval` implements, taken from its positive match arms.
    let supported = matches!(
        expr,
        Expr::Value(_)
            | Expr::Identifier(_)
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
            | Expr::Like { any: false, .. }
    );
    if supported {
        return None;
    }
    let named = |name: &'static str| Some(UnsupportedForm::Expression(Cow::Borrowed(name)));
    match expr {
        Expr::UnaryOp { op, .. } => match op {
            UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus => None,
            other => Some(UnsupportedForm::Expression(Cow::Owned(other.to_string()))),
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
            other => Some(UnsupportedForm::Expression(Cow::Owned(other.to_string()))),
        },
        // The per-request sentinel is the evaluator's own production; the
        // aggregate names are evaluated by the accumulate path, not `eval`.
        Expr::Function(f) => {
            if is_sentinel_call(&f.name, missing_name) || is_allowed_function(&f.name) {
                None
            } else {
                // Named as written. **(corrected 2026-09-10)** This step first
                // prescribed `single_part_name(&f.name).unwrap_or_else(..)`,
                // which lowercases (`engine.rs`), so `LOWER` would report as
                // `lower` and contradict this plan's own corpus rows below.
                // The shipped arm is a case- and quote-preserving single-part
                // lookup, falling back to the qualified `f.name.to_string()`.
                Some(UnsupportedForm::Function(match f.name.0.as_slice() {
                    [ObjectNamePart::Identifier(part)] => part.value.clone(),
                    _ => f.name.to_string(),
                }))
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
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => named("subquery"),
        // **(corrected 2026-09-11)** This arm shipped and was then deleted: no
        // form may have two authorities. `reject_subqueries` (`sql.rs`) already
        // refuses these three variants on every expression before the walk
        // consults the predicate, so the arm was unreachable from both layers —
        // and it spelled a *second* message for one decision
        // (`unsupported expression: subquery` where the pinned text is
        // `unsupported: subquery`). The refusal now lives solely in
        // `reject_subqueries`; the catch-all still refuses the shapes.
        // Anything else keeps the text it produced before this predicate
        // existed — the `Cow` is what makes "no message regression for
        // unnamed variants" structural rather than test-dependent.
        other => Some(UnsupportedForm::Expression(Cow::Owned(other.to_string()))),
    }
}
```

Add to `engine.rs`'s imports: `std::borrow::Cow`, `CastKind` (the predicate's `Cast` arm matches on it), and `crate::sql::is_aggregate_name`. In `sql.rs`, change `fn is_aggregate_name` to `pub(crate) fn is_aggregate_name` — **the signature stays `(&ObjectName) -> bool`**. (The test-module imports were covered in Step 1.)

- [ ] **Step 4: Make `eval`'s catch-all arms consult the predicate**

Replace six arms in `eval` (`engine.rs:895-908`, `:938`, `:978`, `:980-986`, `:987`, `:988`):

```rust
// lines ~895-908, the UnaryOp arm needs an @ binding: the destructured
// `expr` field shadows the whole node, and the predicate must classify the
// UnaryOp itself, not its operand:
un_op @ Expr::UnaryOp { op, expr } => match op {
    // … the Not / Minus / Plus arms are unchanged …
    other => Err(unsupported(un_op, ctx, other.to_string())),
},
// line ~938, the BinaryOp arm (no shadowing here — `expr` still names the
// whole node):
other => Err(unsupported(expr, ctx, other.to_string())),
// line ~978, the Function arm (after the sentinel early return):
match unsupported_form(expr, ctx.missing_name) {
    Some(form) => Err(Error::Unsupported(form.to_string())),
    // The predicate calls this supported — the drift case (an aggregate
    // name reaching `eval`). Keep the internal error it had; the corpus is
    // what catches this.
    None => Err(Error::Value("internal: unexpected aggregate call".into())),
},
// lines ~980-987, the two LIKE arms become three lines. LIKE ANY goes
// through the predicate like everything else — parse time and runtime now
// name the form identically:
Expr::Like { any: true, .. } => Err(unsupported(expr, ctx, || expr.to_string())),
// `any: false` keeps its own arm; the predicate checks the ESCAPE operand
// **(corrected 2026-09-10: the parameter below was dropped — `eval_like`
// carries no `any` backstop, because this arm precedes it and the `any: true`
// row above can never fall through)**:
Expr::Like { negated, any: false, expr, pattern, escape_char } => {
    eval_like(expr, pattern, escape_char, *negated, ctx)
}
// line ~987, the ILike arm — delete it; the catch-all names it through the
// predicate.
// line ~988, the catch-all:
other => Err(unsupported(other, ctx, || other.to_string())),
```

with one helper beside `eval`:

```rust
/// Runtime diagnostic for a form the predicate rejects, falling back to the
/// rendered text when the predicate unexpectedly calls it supported.
/// **(corrected 2026-09-10: the two operand verdicts route to `Parse`, and
/// the fallback is a closure so the live path never renders.)**
fn unsupported(expr: &Expr, ctx: &RowCtx, fallback: impl FnOnce() -> String) -> Error {
    match unsupported_form(expr, ctx.missing_name) {
        Some(UnsupportedForm::Malformed(m)) => Error::Parse(m),
        Some(form) => Error::Unsupported(form.to_string()),
        None => Error::Unsupported(fallback()),
    }
}
```

- [ ] **Step 5: Rewrite the three tests that can no longer call `parse`**

**This is required, not optional: without it the workspace is red as soon as Task 4 lands.** `like_any_is_refused`, `ilike_is_unsupported` and `unknown_function_arm_guards` all build their engine with `Engine::new(parse("…").unwrap())` on queries the validator will refuse at parse time, so the `.unwrap()` panics. Rewrite each to build the plan by hand — the arms stay as the defensive backstop the spec requires, and they keep their coverage: **(corrected 2026-09-10, conformance review)** the inventory was incomplete. `user_sentinel_like_call_is_an_unknown_function` and `delegation_drift_surface`'s `SELECT * EXCEPT (a)` row also had to be rewritten (both now assert the refusal the walk produces), and the follow-up pass added `like_escape_must_be_one_char` / `like_escape_non_string_rejected` to the same list — a refused operand makes their `parse(..).unwrap()` panic too.

```rust
/// A plan whose WHERE carries one expression, built without `parse` — the
/// validator refuses these forms now, so the engine arm needs a direct test.
fn plan_with_where(where_expr: Expr) -> QueryPlan {
    QueryPlan {
        from: FromClause { segments: vec![], alias: Some("s".into()) },
        projections: vec![Projection::Item {
            expr: Expr::Identifier(Ident::new("_1")),
            alias: None,
        }],
        where_expr: Some(where_expr),
        limit: None,
        aggregates: false,
        missing: SentinelNames::mint(),
    }
}

#[test]
fn like_any_is_refused() {
    let mut engine = Engine::new(plan_with_where(Expr::Like {
        negated: false,
        any: true,
        expr: Box::new(Expr::Identifier(Ident::new("_1"))),
        pattern: Box::new(Expr::Value(AstValue::SingleQuotedString("x%".into()).into())),
        escape_char: None,
    }));
    match engine.next(csv(&["x"], &["_1"])) {
        Err(Error::Unsupported(m)) => assert_eq!(m, "unsupported expression: LIKE ANY"),
        other => panic!("expected LIKE ANY unsupported, got {other:?}"),
    }
}
```

Do the same for `ilike_is_unsupported` (`Expr::ILike { .. }`, message `"unsupported expression: ILIKE"` — `ILike` has the same five fields as `Like`, including `any: bool`; set `any: false`). Rename `unknown_function_arm_guards` to `function_arm_drift_guard_keeps_the_internal_error` and change what it drives: an *unknown* function now takes the predicate's `Some` branch (`Error::Unsupported("unsupported function: …")`, covered by the corpus), so the `None` branch it guarded can only be reached by a function the predicate **admits** — an aggregate name in a non-aggregate plan:

```rust
/// The drift guard: the predicate admits the aggregate names (they belong to
/// the scan-accumulate path), so an aggregate reaching `eval`'s Function arm
/// is precisely the case the two sides disagree on — the arm must keep its
/// internal error, proving the backstop still exists.
#[test]
fn function_arm_drift_guard_keeps_the_internal_error() {
    let mut engine = Engine::new(QueryPlan {
        from: FromClause { segments: vec![], alias: Some("s".into()) },
        projections: vec![Projection::Item { expr: func("sum"), alias: None }],
        where_expr: None,
        limit: None,
        aggregates: false,
        missing: SentinelNames::mint(),
    });
    match engine.next(csv(&["x"], &["_1"])) {
        Err(Error::Value(m)) => assert_eq!(m, "internal: unexpected aggregate call"),
        other => panic!("expected the internal error, got {other:?}"),
    }
}
```

Note the message consequence: ILIKE and LIKE ANY now report through the predicate at **both** layers — `unsupported expression: ILIKE` / `unsupported expression: LIKE ANY` — so parse time and runtime name a form identically. The old short strings (`"ILIKE"`, `"LIKE ANY"`) survive nowhere; this is the spec's Q16 message change, asserted in each layer.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p tinio-select`
Expected: PASS, 0 failed, baseline count not shrunk (this task adds 4 tests and rewrites 3 in place).

- [ ] **Step 7: Commit (execute after user confirmation)**

```bash
git add crates/tinio-select/src/engine.rs crates/tinio-select/src/sql.rs
git commit -m "refactor(select): share the supported-expression-form predicate with eval"
```

---

### Task 2: The clause-field, flavor, and empty-projection checks

**Files:**
- Modify: `crates/tinio-select/src/sql.rs` (new functions near the other validators; call sites in `parse()` after the HAVING check at `sql.rs:200`)
- Test: `crates/tinio-select/src/sql.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `fn reject_unread_clauses(query: &Query, select: &Select) -> Result<(), Error>` and `fn reject_empty_projection(select: &Select) -> Result<(), Error>`, private to `sql.rs`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn unread_clauses_are_rejected_on_both_from_paths() {
    // Each construct in BOTH spellings. The custom path may fail earlier
    // (clause ordering) with a different message — both must be rejected;
    // the messages are never compared across paths (spec: legitimately
    // different, and the custom one is known to be less precise).
    let stock = [
        "SELECT TOP 1 s.a FROM S3Object s",
        "SELECT s.a INTO t FROM S3Object s",
        "SELECT s.a FROM S3Object s QUALIFY s.a > 1",
        "SELECT s.a FROM S3Object s WINDOW w AS (PARTITION BY s.a)",
        "SELECT s.a FROM S3Object s FETCH FIRST 1 ROWS ONLY",
        "SELECT s.a FROM S3Object s FOR UPDATE",
        "SELECT s.a FROM S3Object s SETTINGS x = 1",
        "SELECT s.a FROM S3Object s FORMAT CSV",
        "SELECT s.a FROM S3Object s LATERAL VIEW explode(s.b) t AS c",
        "SELECT s.a FROM S3Object s PREWHERE s.a > 1",
        "SELECT s.a FROM S3Object s CONNECT BY PRIOR s.a = s.b",
        "SELECT s.a FROM S3Object s CLUSTER BY s.a",
        "SELECT s.a FROM S3Object s DISTRIBUTE BY s.a",
        "SELECT s.a FROM S3Object s SORT BY s.a",
    ];
    for q in stock {
        assert!(parse(q).is_err(), "must reject: {q}");
        assert!(parse(&q.replace("S3Object s", "S3Object[*] s")).is_err(),
            "must reject the custom spelling of: {q}");
    }
    // Named on the stock path, where the validator sees the AST.
    assert_eq!(
        parse("SELECT TOP 1 s.a FROM S3Object s").unwrap_err().to_string(),
        "S3 select: unsupported: TOP"
    );
}

#[test]
fn from_first_select_is_rejected() {
    // `SelectFlavor::FromFirst` and `::FromFirstNoSelect` — both reachable
    // because GenericDialect::supports_from_first_select() is true.
    for q in ["FROM S3Object s SELECT s.a", "FROM S3Object s"] {
        assert!(parse(q).is_err(), "must reject: {q}");
    }
}

#[test]
fn empty_projection_is_rejected() {
    // GenericDialect::supports_empty_projections() is true.
    assert_eq!(
        parse("SELECT FROM S3Object s").unwrap_err().to_string(),
        "S3 select: empty projection"
    );
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
```

If a custom spelling turns out not to parse at all (the skeleton refuses it before the validator), the `is_err()` assertion still holds — note in a comment which ones fail for that reason.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-select unread_clauses from_first empty_projection -v`
Expected: FAIL — the first three assert `is_err()` on queries that currently parse.

- [ ] **Step 3: Add the checks**

```rust
/// Every `Select`/`Query` field the engine does not read must be at its
/// default. `QueryPlan` carries six fields, so a clause with no field cannot
/// reach the engine — accepting one would silently drop it. Every field
/// listed here is non-AWS syntax, so over-rejecting is impossible.
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
    let unsupported = |name: &str| Err(Error::Unsupported(name.into()));
    if select.top.is_some() || select.top_before_distinct {
        return unsupported("TOP");
    }
    if select.into.is_some() {
        return unsupported("INTO");
    }
    if select.qualify.is_some() {
        return unsupported("QUALIFY");
    }
    if !select.named_window.is_empty() || select.window_before_qualify {
        return unsupported("WINDOW");
    }
    if !select.lateral_views.is_empty() {
        return unsupported("LATERAL VIEW");
    }
    if select.prewhere.is_some() {
        return unsupported("PREWHERE");
    }
    if !select.connect_by.is_empty() {
        return unsupported("CONNECT BY");
    }
    if !select.cluster_by.is_empty() {
        return unsupported("CLUSTER BY");
    }
    if !select.distribute_by.is_empty() {
        return unsupported("DISTRIBUTE BY");
    }
    if !select.sort_by.is_empty() {
        return unsupported("SORT BY");
    }
    if query.fetch.is_some() {
        return unsupported("FETCH");
    }
    if !query.locks.is_empty() {
        return unsupported("FOR UPDATE");
    }
    // (amended 2026-09-11) Split from `locks`: MSSQL's FOR XML/FOR JSON/
    // FOR BROWSE are not the lock clause, so each field reports its own name.
    if query.for_clause.is_some() {
        return unsupported("FOR");
    }
    if query.settings.is_some() {
        return unsupported("SETTINGS");
    }
    if query.format_clause.is_some() {
        return unsupported("FORMAT");
    }
    if !query.pipe_operators.is_empty() {
        return unsupported("PIPE");
    }
    // Dead under GenericDialect (Redshift-only / BigQuery-only gates, and the
    // trait default for select_modifiers) — asserted as future-proofing so a
    // dialect change cannot open a silent hole.
    if select.exclude.is_some() {
        return unsupported("EXCLUDE");
    }
    if select.select_modifiers.is_some() {
        return unsupported("SELECT modifier");
    }
    if select.value_table_mode.is_some() {
        return unsupported("value table");
    }
    if select.flavor != SelectFlavor::Standard {
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
```

In `parse()`, immediately after the HAVING check (`sql.rs:200`) and before the LIMIT block:

```rust
    reject_unread_clauses(query, select)?;
    reject_empty_projection(select)?;
```

Add `SelectFlavor` **and `Query`** to the `sqlparser::ast` imports in `sql.rs` (neither is imported today — the existing code only destructures `Statement::Query(query)` without naming the type).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-select`
Expected: PASS, 0 failed, baseline count not shrunk — including the pre-existing `ORDER BY`/`GROUP BY`/`HAVING`/`DISTINCT` reject tests.

- [ ] **Step 5: Commit (execute after user confirmation)**

```bash
git add crates/tinio-select/src/sql.rs
git commit -m "fix(select): reject clauses the engine does not read"
```

---

### Task 3: The wildcard-option check

**Files:**
- Modify: `crates/tinio-select/src/sql.rs` (new function; call site in `parse()` right after `reject_empty_projection`)
- Test: `crates/tinio-select/src/sql.rs`

**Interfaces:**
- Consumes: the Task 2 call sites.
- Produces: `fn reject_wildcard_options(select: &Select) -> Result<(), Error>`, private to `sql.rs`.

- [ ] **Step 1: Write the failing tests**

```rust
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
    // check is NOT a no-op there (probe-verified 2026-09-10).
    for q in [
        "SELECT * EXCLUDE (a) FROM S3Object[*] s",
        "SELECT * EXCEPT (a) FROM S3Object[*] s",
    ] {
        assert!(parse(q).is_err(), "must reject: {q}");
    }
}

#[test]
fn plain_wildcards_still_parse() {
    for q in ["SELECT * FROM S3Object s", "SELECT s.a, * FROM S3Object s"] {
        assert!(parse(q).is_ok(), "must accept: {q}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-select wildcard_options -v`
Expected: FAIL — all five options currently parse.

- [ ] **Step 3: Add the check**

```rust
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
        if opts.opt_ilike.is_some() {
            return Err(Error::Unsupported("ILIKE".into()));
        }
        if opts.opt_exclude.is_some() {
            return Err(Error::Unsupported("EXCLUDE".into()));
        }
        if opts.opt_except.is_some() {
            return Err(Error::Unsupported("EXCEPT".into()));
        }
        if opts.opt_replace.is_some() {
            return Err(Error::Unsupported("REPLACE".into()));
        }
        if opts.opt_rename.is_some() {
            return Err(Error::Unsupported("RENAME".into()));
        }
    }
    Ok(())
}
```

Call it in `parse()` directly after `reject_empty_projection(select)?;`.

The function-argument site (`FunctionArgExpr::WildcardWithOptions`) needs no check: `validate_aggregate_item` already refuses the shape (probe: `SELECT count(* EXCLUDE (a)) FROM S3Object s` → rejected), and every non-aggregate function is refused by Task 4.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-select`
Expected: PASS, 0 failed.

- [ ] **Step 5: Commit (execute after user confirmation)**

```bash
git add crates/tinio-select/src/sql.rs
git commit -m "fix(select): reject wildcard options instead of dropping them"
```

---

### Task 4: The expression walk

**Files:**
- Modify: `crates/tinio-select/src/sql.rs` (new visitor; call site in `parse()`)
- Test: `crates/tinio-select/src/sql.rs`

**Interfaces:**
- Consumes: `engine::unsupported_form(&Expr, &str) -> Option<UnsupportedForm>` (Task 1) — **nothing else**; the sentinel and the function allowlist are handled inside the predicate, so `sql.rs` holds no second copy of either.
- Produces: `fn reject_unsupported_expressions(select: &Select, missing: &SentinelNames) -> Result<(), Error>`, private to `sql.rs`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn unknown_functions_and_forms_are_rejected_at_request_level() {
    let cases = [
        ("SELECT LOWER(s.a) FROM S3Object s", "unsupported function: LOWER"),
        ("SELECT COALESCE(s.a, 'x') FROM S3Object s", "unsupported function: COALESCE"),
        ("SELECT CAST(s.a AS INT) FROM S3Object s", "unsupported expression: CAST"),
        ("SELECT CASE WHEN s.a > 1 THEN 1 ELSE 0 END FROM S3Object s",
         "unsupported expression: CASE"),
        ("SELECT s.a FROM S3Object s WHERE SUBSTRING(s.a, 1) = 'x'",
         "unsupported expression: SUBSTRING"),
        ("SELECT unknown_fn(s._1) FROM S3Object s", "unsupported function: unknown_fn"),
        ("SELECT s.a FROM S3Object s WHERE NOW() = s.a", "unsupported function: NOW"),
        ("SELECT * FROM S3Object s WHERE s._1 ILIKE 'X'", "unsupported expression: ILIKE"),
        ("SELECT * FROM S3Object s WHERE s._1 LIKE ANY 'x%'",
         "unsupported expression: LIKE ANY"),
    ];
    for (q, msg) in cases {
        assert_eq!(
            parse(q).unwrap_err().to_string(),
            format!("S3 select: unsupported: {msg}"),
            "for {q}"
        );
    }
}

#[test]
fn the_walk_runs_before_the_aggregate_shape_check() {
    // Q7: the deeper diagnosis wins. `count(LOWER(x))` is a bare aggregate
    // call whose argument is not a plain expression, so the aggregate-shape
    // check would report "non-aggregate expression in aggregate select list";
    // the walk runs first and names the actual mistake.
    assert_eq!(
        parse("SELECT count(LOWER(s.a)) FROM S3Object s")
            .unwrap_err()
            .to_string(),
        "S3 select: unsupported: unsupported function: LOWER"
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
    for q in ["SELECT s.CAST FROM S3Object s", "SELECT s.date FROM S3Object s"] {
        assert!(parse(q).is_ok(), "must accept: {q}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-select unknown_functions the_walk_runs keyword_shaped -v`
Expected: FAIL — the queries in the first two tests currently parse.

- [ ] **Step 3: Add the walk**

```rust
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
/// refusal and the runtime error from ever disagreeing.
fn reject_unsupported_expressions(
    select: &Select,
    missing: &SentinelNames,
) -> Result<(), Error> {
    struct Walk<'a> {
        missing_name: &'a str,
        found: Option<Error>,
    }
    impl Visitor for Walk<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(form) = engine::unsupported_form(expr, self.missing_name) {
                self.found = Some(Error::Unsupported(form.to_string()));
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    // is_missing() returns an owned String — bind it before borrowing,
    // or the temporary does not live long enough for the `&'a str` field.
    let missing_name = missing.is_missing();
    let mut walk = Walk {
        missing_name: &missing_name,
        found: None,
    };
    let mut targets: Vec<&Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(e) => Some(e),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr),
            _ => None,
        })
        .collect();
    if let Some(where_expr) = &select.selection {
        targets.push(where_expr);
    }
    for expr in targets {
        let _ = expr.visit(&mut walk);
        if let Some(err) = walk.found.take() {
            return Err(err);
        }
    }
    Ok(())
}
```

Add `use crate::engine;` — `ControlFlow`, `Visit` and `Visitor` are **already imported** at `sql.rs:5-14` (the set `reject_subqueries` uses), so no further imports are needed here.

Call it in `parse()` **immediately after the projection loop (`sql.rs:264`) and before the `aggregates && Wild` check** — Q7: the deeper diagnosis must win over the aggregate-shape message:

```rust
    reject_unsupported_expressions(select, &missing)?;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-select`
Expected: PASS, 0 failed — including every `IS MISSING` test in the existing corpus.

- [ ] **Step 5: Commit (execute after user confirmation)**

```bash
git add crates/tinio-select/src/sql.rs
git commit -m "fix(select): refuse unsupported expression forms at parse time"
```

---

### Task 5: Parquet `DATE`

**Files:**
- Modify: `crates/tinio-select/src/parquet.rs` (two arms in `arrow_value`, before the `other =>` catch-all at `parquet.rs:239-243`; two helpers near `timestamp_string` at `parquet.rs:369`)
- Test: `crates/tinio-select/src/parquet.rs` (`#[cfg(test)]` module; add `Date32Array`/`Date64Array` to its `use` block)

**Interfaces:**
- Consumes: `one_column(ArrowField, ArrayRef) -> Vec<u8>`, `reader(Vec<u8>) -> ParquetReader`, `record(&mut ParquetReader) -> Record` (existing test helpers, `parquet.rs:515-536`).
- Produces: `fn date_string_from_days(days: i64) -> Result<String, Error>` and `fn date_string_from_millis(ms: i64) -> Result<String, Error>`, private to `parquet.rs`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn date32_maps_to_midnight_utc() {
    // 19723 days since the Unix epoch = 2024-01-01.
    let array = Arc::new(Date32Array::from(vec![Some(19723)])) as ArrayRef;
    let bytes = one_column(ArrowField::new("d", DataType::Date32, true), array);
    let Record::Parquet(Columns { fields, .. }) = record(&mut reader(bytes)) else {
        panic!("expected parquet record")
    };
    assert_eq!(
        fields[0],
        Field::Present(Value::String("2024-01-01T00:00:00Z".into()))
    );
}

#[test]
fn date32_pre_epoch_is_negative() {
    // -1 = 1969-12-31 — floor division, not truncation.
    let array = Arc::new(Date32Array::from(vec![Some(-1)])) as ArrayRef;
    let bytes = one_column(ArrowField::new("d", DataType::Date32, true), array);
    let Record::Parquet(Columns { fields, .. }) = record(&mut reader(bytes)) else {
        panic!("expected parquet record")
    };
    assert_eq!(
        fields[0],
        Field::Present(Value::String("1969-12-31T00:00:00Z".into()))
    );
}

#[test]
fn date64_floors_to_the_day() {
    // A non-midnight millisecond value must yield that day's midnight: the
    // type disclaims time-of-day, so the arm enforces it rather than leaking
    // whatever the input happened to carry.
    let noon = 19723i64 * 86_400_000 + 12 * 3_600_000;
    let array = Arc::new(Date64Array::from(vec![Some(noon)])) as ArrayRef;
    let bytes = one_column(ArrowField::new("d", DataType::Date64, true), array);
    let Record::Parquet(Columns { fields, .. }) = record(&mut reader(bytes)) else {
        panic!("expected parquet record")
    };
    assert_eq!(
        fields[0],
        Field::Present(Value::String("2024-01-01T00:00:00Z".into()))
    );
}

#[test]
fn out_of_range_dates_are_format_errors() {
    // Never a panic or a wrap.
    let array = Arc::new(Date32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
    let bytes = one_column(ArrowField::new("d", DataType::Date32, true), array);
    // ParquetReader::next is `Result<Option<Record>, Error>` — the error is
    // the OUTER variant, not nested in an Option.
    match reader(bytes).next() {
        Err(Error::Format(_)) => {}
        other => panic!("expected format error, got {other:?}"),
    }
}
```

`Record::Parquet(Columns { fields, .. })` is the module's existing assertion idiom (`parquet.rs:541-552`) — this module has no `s(..)` helper; assert the `Field::Present(Value::…)` shape directly. `Columns` and `Record` are already imported by the test module.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-select --features parquet date32 date64 out_of_range -v`
Expected: FAIL — `parquet type not supported: Date32`.

- [ ] **Step 3: Add the arms and helpers**

Two arms in `arrow_value`, before the `other =>` catch-all:

```rust
DataType::Date32 => {
    Value::String(date_string_from_days(primitive::<Date32Type>(array, i) as i64)?)
}
DataType::Date64 => Value::String(date_string_from_millis(primitive::<Date64Type>(array, i))?),
```

Helpers beside `timestamp_string`:

```rust
/// Days since the Unix epoch → RFC 3339 midnight UTC. Chosen over a bare
/// `YYYY-MM-DD` so DATE and TIMESTAMP share one shape on the service's
/// string spine until a real timestamp type exists (spec Q4).
///
/// **(corrected 2026-09-10, conformance review)** This step first prescribed an
/// i64 *nanosecond* intermediate below, which caps the range at ±292 years of
/// the epoch and fails a legal DATE past 2262-04-11. The offset is built in
/// whole days of *seconds* instead, so `time`'s own ±9999-year span is the
/// only bound.
fn date_string_from_days(days: i64) -> Result<String, Error> {
    let out_of_range = || Error::Format("parquet date out of range".into());
    let offset = time::Duration::seconds(days.checked_mul(86_400).ok_or_else(out_of_range)?);
    let dt = OffsetDateTime::UNIX_EPOCH
        .checked_add(offset)
        .ok_or_else(out_of_range)?;
    dt.format(&Rfc3339)
        .map_err(|e| Error::Format(format!("parquet date format: {e}")))
}

/// Milliseconds → that day's midnight UTC. `div_euclid` floors toward
/// negative infinity, so a pre-epoch value lands on the right day.
fn date_string_from_millis(ms: i64) -> Result<String, Error> {
    date_string_from_days(ms.div_euclid(86_400_000))
}
```

Add `Date32Type`/`Date64Type` to the non-test `arrow::datatypes` imports (beside the existing `Int64Type`). `OffsetDateTime` and `Rfc3339` are already imported at `parquet.rs:46`.

- [ ] **Step 4: Add the AWS per-type entries**

Extend the existing `reads_mapped_types` coverage with one case per AWS-listed type, adding `Int8`/`Int16` (already mapped, previously untested) and a `Dictionary` case asserting `Error::Format`. **(corrected 2026-09-10, conformance review)** The ENUM note must say the opposite of what this step first asked for: `parquet` 59.3.0 maps `LogicalType::Enum` and `ConvertedType::ENUM` alike to `DataType::Binary` (`src/arrow/schema/primitive.rs:281`, `:288`), so an ENUM column is **refused** as `Format("parquet type not supported: Binary")`, not mapped via `Utf8`. Still unverifiable in-tree — arrow has no ENUM data type, so `ArrowWriter` cannot produce one, and the repo has no `.parquet` fixtures — so the comment records the source-verified mapping rather than a guess.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p tinio-select --features parquet`
Expected: PASS, 0 failed.
Run: `cargo test -p tinio-select`
Expected: PASS — the parquet module is not compiled without the feature, so the count is unchanged from Task 4's, which is exactly why the feature must be passed explicitly.

- [ ] **Step 6: Commit (execute after user confirmation)**

```bash
git add crates/tinio-select/src/parquet.rs
git commit -m "feat(select): map parquet DATE columns"
```

---

### Task 6: The FR-034 contract and the reserved-keyword pin

**Files:**
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (the `SelectObjectContent` FR-034 bullet, `s3-surface.md:40`)
- Test: `crates/tinio-select/src/json.rs` (**`#[cfg(test)]` module only** — it has the `run(query, type, input)` helper at `json.rs:299` and `s(..)` at `json.rs:335`)

**Interfaces:**
- Consumes: Task 2/3/4's refusals (documented) and the existing `run` helper.
- Produces: no code interface — a documentation change plus one pin.

- [ ] **Step 1: Write the pin**

In `json.rs`'s test module:

```rust
#[test]
fn reserved_keyword_attribute_is_accepted() {
    // Recorded divergence (spec A2): AWS answers 400 for an unquoted reserved
    // keyword used as an attribute name and requires s."CAST"; we accept it,
    // and MinIO documents the same non-compliance ("AWS S3's reserved
    // keywords list is not yet respected"). Pinned at the EVALUATION layer,
    // not at parse — a parse-level `Ok` assertion would keep passing when
    // the scalar-function spec turns CAST into a real function, which is
    // exactly the change that should force a re-decision here.
    let rows = run(
        "SELECT s.CAST FROM S3Object s",
        Type::Lines,
        "{\"CAST\": \"x\"}\n",
    );
    assert_eq!(rows[0].vals, vec![s("x")]);
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p tinio-select reserved_keyword_attribute -v`
Expected: PASS — this pins existing behaviour rather than driving new code. **If it fails, Task 4's walk has become text-based; that is the guard working.**

- [ ] **Step 3: Update the FR-034 contract**

In `specs/001-s3-local-server/contracts/s3-surface.md`, extend the `SelectObjectContent` bullet with: the SQL acceptance surface (the clause-field allowlist, the wildcard options, the `FROM`-first and empty-projection refusals, the expression/form walk, the function allowlist, the sentinel exception, and the operand checks — literals, `ESCAPE`) and the named refusals mapping to 400 `S3QueryParsingError`; the `optimizer_hints` exception (accepted and ignored, a hint being advisory); the reserved-keyword divergence with its rationale and the MinIO precedent; the `B'…'`/`R'…'` silent divergence; and the Parquet `ENUM` note. **(corrected 2026-09-10, conformance review)** The ENUM note is a **refusal**, not "believed supported via `Utf8`": `parquet` 59.3.0 maps ENUM to `DataType::Binary` and this module's catch-all refuses `Binary` as `Format`.

- [ ] **Step 4: Run the whole crate**

Run: `cargo test -p tinio-select`
Expected: PASS, 0 failed.

- [ ] **Step 5: Commit (execute after user confirmation)**

```bash
git add specs/001-s3-local-server/contracts/s3-surface.md crates/tinio-select/src/json.rs
git commit -m "docs(select): record the AWS acceptance surface and the reserved-keyword divergence"
```

---

### Task 7: The e2e scenario and whole-workspace verification

**Files:**
- Modify: `crates/tinio-e2e/tests/features/select.feature` (one new scenario — no tag needed; the file already carries a feature-level `@FR-034`)
- Test: the cucumber suite

**Interfaces:**
- Consumes: everything above.
- Produces: nothing downstream.

- [ ] **Step 1: Add the scenario**

Append to `select.feature`, using the file's existing step vocabulary verbatim — this is the shape the bad-SQL scenario already uses at `select.feature:129-136`:

```gherkin
  Scenario: A refused clause and an unknown function are request-level parse errors
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,x
      """
    When I try select over object "data.csv" with query "SELECT TOP 1 s._1 FROM S3Object s"
    Then the select fails with HTTP 400 and code "S3QueryParsingError"
    When I try select over object "data.csv" with query "SELECT LOWER(s._1) FROM S3Object s"
    Then the select fails with HTTP 400 and code "S3QueryParsingError"
```

The exact step spellings are `I try select over object "X" with query "Y"` and `the select fails with HTTP 400 and code "Z"`; the object body is a `"""` block. Do not invent step text.

- [ ] **Step 2: Run the crate suites**

Run: `cargo test -p tinio-select && cargo test -p tinio-select --features parquet && cargo test -p tinio-server`
Expected: PASS, 0 failed.

- [ ] **Step 3: Run the e2e suite — both backends, plus traceability**

The `docs/tests.md:44-48` commands verbatim — no `--profile ci` (that profile is for the CI runners, `docs/tests.md:11`) and no `--retry` (retry belongs to the `@interop` leg, and `select.feature` is not `@interop`):

```bash
cargo test -p tinio-e2e
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber \
  -- --tags 'not @fs and not @parquet and not @interop and not @boto3 and not @mc'
cargo test -p tinio-e2e --test traceability
```

**(corrected 2026-09-11)** The mem command must re-state `not @parquet`. An explicit
`--tags` **replaces** the runner's default filter (`tests/cucumber.rs`), it does not
intersect with it — so when a later feature file gained a `@parquet` scenario, this
command became the one place the tag was not excluded, and the leg failed with
`501 NotImplemented: Parquet input requires the select-parquet feature`. `docs/tests.md`
and `crates/tinio-e2e/README.md` were both updated when the tag landed; this plan was not,
which is why the command here is the copy that misled.

Expected: the select feature passes on both legs. The **traceability leg is separate** and must be run because this task changes a feature file — it is the check that every tagged scenario maps to a registered FR id (it accepts no `--tags`/`--retry`, `docs/tests.md:40`).

- [ ] **Step 4: Whole-workspace check**

Run: `cargo test --workspace`
Expected: PASS, 0 failed — the gate proving Task 1's `engine.rs` refactor did not disturb evaluation.

- [ ] **Step 5: Commit (execute after user confirmation)**

```bash
git add crates/tinio-e2e/tests/features/select.feature
git commit -m "test(select): pin the acceptance-surface refusals end to end"
```

---

## Self-Review

**Spec coverage.** A1.1 clause fields → Task 2; A1.2 wildcard options → Task 3; A1.3 empty projection → Task 2; the `flavor` assertion → Task 2; the `optimizer_hints` exception → Task 2 (code) + Task 6 (contract); A3 two tables, the shared predicate, the function allowlist, the sentinel exception, form-not-text → Tasks 1 and 4; the parity corpus → Task 1's `unsupported_form_corpus`; A4 `Date32`/`Date64`, the floor rule, out-of-range, `Dictionary`, the `--features parquet` requirement → Task 5; A2 divergence + eval-layer pin → Task 6; the e2e scenario → Task 7. "No new FR number / `traceability.rs` unchanged" is honoured — Task 7 reuses the feature-level `@FR-034`.

**Placeholders.** None. The earlier empty `parity_corpus_predicate_agrees_with_eval` stub is gone, replaced by Task 1's table-driven `unsupported_form_corpus`; Task 7's "as `docs/tests.md` prescribes" is replaced by the `docs/tests.md:44-48` commands verbatim; the `"CASE_NEVER"` red-step artifact is removed (Task 1's `Cast` assertion names `"CAST"` outright — the test still fails before Step 3 lands, because `unsupported_form` does not exist yet).

**Type consistency.** `unsupported_form(&Expr, &str) -> Option<UnsupportedForm>` is defined in Task 1 and consumed verbatim in Task 4; `UnsupportedForm`'s `Display` shapes are asserted in Task 1 and relied on by Tasks 4 and 6. `is_allowed_function(&ObjectName)` takes the same type as `is_aggregate_name(&ObjectName)` — no `&str` round trip. `is_aggregate_name` is made `pub(crate)` in Task 1 with its signature unchanged. `is_sentinel_call` is an existing `engine.rs` item, reused not redefined; **(corrected 2026-09-10)** `single_part_name` is *not* reused — it lowercases, which contradicts this plan's own `unsupported function: LOWER` rows, so the Function arm keeps the name as written. `SentinelNames::is_missing()` returns an owned `String` — both call sites bind/borrow it explicitly. `TRY_CAST`/`SAFE_CAST` are `CastKind`s, matched inside the `Cast` arm. The UnaryOp catch-all classifies the whole node through an `un_op @` binding because the destructured field shadows it. LIKE ANY and ILIKE report through the predicate at both layers; the drift guard is driven by an admitted aggregate name (`func("sum")`), the only shape that reaches the `None` branch.

**Verified against the repo before planning**, not assumed: `Expr::Wildcard` is unreachable from the visitor; the three engine tests that must be rewritten; `SentinelNames::mint`/`FromClause`/`QueryPlan` constructibility; the exact cucumber commands and the separate traceability target; `select.feature`'s single feature-level tag.

---

## Addendum — conformance-review pass (2026-09-10)

A review of the shipped Tasks 1-7 (implementation vs this plan and its design) found the acceptance surface still open on the *operand-carrying* forms, one stale fact, and one range bug. The rulings taken with the user and their implementation are recorded in the design's "Conformance-review follow-up" section; in this plan's terms they are:

- **Task 1's predicate gains two verdicts.** `UnsupportedForm::Literal` (a literal kind with no mapping — `X'…'`, `N'…'`, `$$…$$`) and `UnsupportedForm::Malformed` (a numeric literal outside the decimal model, or a bad `ESCAPE`). The `Expr::Value` and `Expr::Like { any: false }` arms run the evaluator's own `literal`/`like_escape` and keep their message; Task 4's walk maps `Malformed` to `Error::Parse` and everything else to `Error::Unsupported`. This closes the three classes that used to parse, stream, and fail per row.
- **The unnamed-variant echo is bounded (256 chars, an appended `...` marking the cut) and has the request's sentinel name replaced by `IS MISSING`** — a rendered subtree can carry the sentinel, and the client should not see the evaluator's per-request name. This also makes Task 1's `unsupported(..)` fallback lazy.
- **Task 5's `date_string_from_days` is corrected** to a whole-day offset in *seconds* (see the snippet above), and `dates_past_2262_still_read` pins the widened range.
- **The ENUM notes in Task 5 Step 4 and Task 6 Step 3 are corrected** (see above): an ENUM column is refused, not mapped. **(superseded 2026-09-11 — see the design doc's A4)** The annotation *is* recoverable, from the parquet schema rather than the arrow array, so a top-level ENUM now reads as a string; only a nested ENUM, a non-UTF-8 ENUM cell and a duplicate name mixing ENUM with an unannotated `Binary` stay refused.
- **Task 2's empty-projection test is extended** with the custom-path spelling, which dies in the parser rather than in the validator — the alternative the design allowed, now pinned rather than implicit.
- **Left alone, deliberately:** the `FOR UPDATE` label for `Query.locks`/`for_clause` (Task 2's table maps both that way), and `B'…'`/`R'…'`, which the lexer never turns into literals and which the FR-034 contract records as a silent divergence. Closing them means `dialect.rs`, which the Global Constraints keep out of scope. **(both superseded 2026-09-11)** The label split — `for_clause` reports `FOR` (MSSQL's `FOR XML`/`FOR JSON`/`FOR BROWSE` are not the lock clause) — and `B'…'`/`R'…'` closed via a one-method `Dialect::dialect()` identity override in `dialect.rs`: the tokenizer's byte/raw-string gates then read as `GenericDialect`, so both tokenize as literals and the operand check refuses them by name. The closure and its side-effects (`b'…'`, the quoted forms, the `CURRENT_USER` family) are recorded in the FR-034 contract; the Global Constraints' `dialect.rs` line above carries the breach note.

Baselines after the pass: `cargo test -p tinio-select` → 306 + 2 doctests (was 284 + 2); `--features parquet` → 328 + 2.
