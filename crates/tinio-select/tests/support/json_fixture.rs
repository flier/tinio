//! The synthetic-element shape the JSON DOCUMENT fixtures share. The bench
//! (`benches/select_scan.rs`) and the large-input streaming test
//! (`src/json/mod.rs`, at file scope rather than inside its `tests` module —
//! that declaration's own comment says why) each `#[path]`-include this file and
//! build their own document around it, because only the layout differs: the
//! bench keeps its fixture under the 1 MB whole-document cap so the
//! pre-refactor implementation can read it too, while the test pretty-prints
//! and gzips.
//!
//! Each binary uses one of the two renderings, so the other is dead there.
#![allow(dead_code)]

/// The zero-padded name both fixtures give element `i`.
fn name(i: u64) -> String {
    format!("record-{i:08}")
}

/// The second tag both fixtures give element `i`.
fn tag(i: u64) -> String {
    format!("beta-{i:08}")
}

/// Element `i` as one compact JSON object — the bench's rendering.
pub fn element_compact(i: u64) -> String {
    format!(
        "{{\"id\": {i}, \"name\": \"{}\", \"price\": {}, \"tags\": [\"alpha\", \"{}\"]}}",
        name(i),
        i % 997,
        tag(i),
    )
}

/// Element `i` pretty-printed over four lines — the large test's rendering.
/// Its `price` carries a fractional part where the bench's does not.
pub fn element_pretty(i: u64) -> String {
    format!(
        "  {{\n    \"id\": {i},\n    \"name\": \"{}\",\n    \"price\": {}.5,\n    \"tags\": [\"alpha\", \"{}\"]\n  }}",
        name(i),
        i % 997,
        tag(i),
    )
}
