//! Row-value mapping and object key lookup for the engine's select side. The
//! streaming traversal in `matcher` cannot use the map-based `json_lookup` —
//! it applies the same case-folding and ambiguity rule directly to the
//! `MapAccess` stream.

use serde_json::Value;

use crate::{
    error::Error,
    row::{NameStyle, RowCtx, Value as RowValue},
};

/// The JSON element of a row in the row model: numbers keep their verbatim
/// token (`RawNumber`, arbitrary precision), scalars map to their variants,
/// objects/arrays become `Json` values.
pub(crate) fn to_value(j: &Value) -> RowValue {
    match j {
        Value::Null => RowValue::Null,
        Value::Bool(b) => RowValue::Bool(*b),
        Value::Number(n) => RowValue::RawNumber(n.to_string()),
        Value::String(s) => RowValue::String(s.clone()),
        Value::Array(_) | Value::Object(_) => RowValue::Json(Box::new(j.clone())),
    }
}

/// Does `key` name `name` under `style`? The one comparison the engine's
/// select-side lookup and the streaming traversal share — review 2026-09-06b
/// R10 exists because those two must not drift.
pub(crate) fn key_matches(key: &str, name: &str, style: NameStyle) -> bool {
    if style.exact() {
        key == name
    } else {
        key.eq_ignore_ascii_case(name)
    }
}

/// Object key lookup: case-insensitive when unquoted, exact when quoted;
/// a case-folded duplicate is ambiguous (AWS: two attrs differing only in
/// case → `AmbiguousFieldName`). The engine's select-side lookup; the
/// streaming traversal applies the same rule in `matcher`, and review
/// 2026-09-06b R10 exists because those two must not drift.
///
/// JSON keys are per-row and per-nesting-level (`serde_json::Map`), so there
/// is no index to build the way CSV/parquet do; this is the one lookup whose
/// cost stays linear in the map, and the request budget `ctx` carries is what
/// bounds it (P3) — one charge per entry scanned, before the case rules run.
pub(crate) fn json_lookup<'a>(
    map: &'a serde_json::Map<String, Value>,
    name: &str,
    style: NameStyle,
    ctx: &RowCtx,
) -> Result<Option<&'a Value>, Error> {
    // Early-exit count (X8): two matches is the ambiguity verdict — a
    // Vec-collect per lookup was pure allocation on the hot path.
    let mut found: Option<&Value> = None;
    for (key, value) in map {
        ctx.budget.charge(1)?;
        if key_matches(key, name, style) {
            if found.is_some() {
                return Err(Error::Ambiguous(name.into()));
            }
            found = Some(value);
        }
    }
    Ok(found)
}
