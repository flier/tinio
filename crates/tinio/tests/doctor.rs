//! T057's residue: the doctor's `--json` document against `contracts/doctor-report.schema.json`.
//!
//! **The flows are `crates/tinio-e2e/tests/features/doctor.feature` now** (2026-10-01 test
//! placement cleanup, T6): the plain → `--dry-run` → `--fix` journeys, the crashed/defective
//! fixtures and both renderings are cucumber scenarios, driven through the same built binary. What
//! stays is the one leg `docs/tests.md`'s T057 gate names — the **key set** the schema fixes
//! (`additionalProperties: false` at both levels), which no scenario asserts: the step layer
//! carries no JSON reader (`crates/tinio-e2e/tests/steps/cli.rs`'s `report_severity` reads one
//! field by text and says why), and a hand-rolled key-set comparison would be a second spelling of
//! the encoding the schema is written against. The unit half of the same rule is
//! `the_json_rendering_is_the_same_report` in `crates/tinio-cli/src/report.rs`.
//!
//! The leg only: the root it diagnoses and the spawn shape live in [`common`], which this target
//! declares below.

// The leg spells `std::fs`/`std::process` at the call site rather than importing each one; this is
// a test crate and the absolute paths are the point (they name the module, not a local alias), the
// same shape `common/` and `commands::doctor::tests` use.
#![allow(clippy::absolute_paths)]

mod common;

use common::*;
use tinio_util::testing::json_keys;

#[test]
fn the_json_report_is_the_schema_shape() {
    // The normative half of the shape is `contracts/doctor-report.schema.json`; this is the
    // implementation half, and a field added on one side only must fail the other. The fixture's
    // root **has** a store, so this is the key set on a populated report: the schema's
    // `additionalProperties: false` applies to a finding's own object too, and a report with no
    // findings would leave that half of it unread.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = run_doctor(dir.path(), &root, &["--json"]);
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(json_keys(&parsed), ["findings", "severity"]);
    assert!(
        matches!(parsed["severity"].as_str(), Some("ok" | "warn" | "error")),
        "{parsed}"
    );
    let findings = parsed["findings"].as_array().expect("findings");
    assert!(findings.len() > 1, "{parsed}");
    for finding in findings {
        assert_eq!(
            json_keys(finding),
            ["check", "detail", "severity"],
            "{finding}"
        );
        assert!(
            matches!(finding["severity"].as_str(), Some("ok" | "warn" | "error")),
            "{finding}"
        );
        assert!(finding["check"].is_string(), "{finding}");
        assert!(finding["detail"].is_string(), "{finding}");
    }
}
