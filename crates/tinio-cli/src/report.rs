//! The doctor's report: one finding per check, two renderings, one exit code.
//!
//! `--json` is the same report as `render_text`, not a second pass over the filesystem
//! (design §4): the checks run once and the renderer is what differs.

use std::fmt::Write;

use parse_display::Display;
use serde::Serialize;

use crate::{
    _core::cleanup::{RepairAction, RepairActionLevel},
    Error,
};

/// The severity a finding carries, mapped from the engine's four levels.
///
/// `Display` and the JSON spelling are the **same** derivation — the variant's name, lowercased —
/// rather than a hand-written word table beside `serde`'s: one place spells the variants, and
/// `every_severity_has_one_word_in_both_renderings` pins the two renderings to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Display)]
#[serde(rename_all = "lowercase")]
#[display(style = "lowercase")]
pub enum Severity {
    /// Nothing to do.
    Ok,
    /// Something the operator should look at, repaired or not.
    Warn,
    /// Something that could not be repaired.
    Error,
}

/// The name of the private-state check, whose rows the cleanup stream produces here and the store
/// legs produce in the check list: one name, so the leg reads the same in either rendering
/// whichever of them wrote the row.
pub const PRIVATE_STATE: &str = "private state";

/// One check's answer.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    /// What was checked.
    pub check: String,
    /// How bad it is.
    pub severity: Severity,
    /// What was found, in one line.
    pub detail: String,
}

impl Finding {
    /// A check's own finding.
    pub fn check(check: impl Into<String>, severity: Severity, detail: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            severity,
            detail: detail.into(),
        }
    }

    /// A repair action as a finding (`Lint`/`Info` are findings, `Warn`/`Error` are problems).
    ///
    /// Takes the action by value: it is consumed into the row, and every caller is done with it
    /// afterwards, so the description moves rather than being copied.
    pub fn from_action(action: RepairAction) -> Self {
        let severity = match action.level {
            RepairActionLevel::Lint | RepairActionLevel::Info => Severity::Ok,
            RepairActionLevel::Warn => Severity::Warn,
            RepairActionLevel::Error => Severity::Error,
        };
        Self {
            check: PRIVATE_STATE.to_string(),
            severity,
            detail: action.description,
        }
    }

    /// A cleanup **proposal** as a plain run's informational row: the same row as
    /// [`Self::from_action`], at [`Severity::Ok`], because a "would …" describes what a fix
    /// would change rather than a defect the root has. `--dry-run` renders that same action
    /// through [`Self::from_action`] instead, so the two modes differ by this one mapping and
    /// by nothing else — which is what makes `--dry-run`'s list exactly `--fix`'s list
    /// (the doctor section's `--dry-run` sentence). A stage that *failed* never comes through here: failures ride
    /// the action stream's `Err` side and are `Severity::Error` in both modes.
    pub fn informational(action: RepairAction) -> Self {
        Self {
            severity: Severity::Ok,
            ..Self::from_action(action)
        }
    }
}

/// Every finding of one run.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// The findings, in the order the checks produced them. Private, so `findings()` is the only
    /// way to read them.
    findings: Vec<Finding>,
}

/// The report as `--json` writes it: the same findings, with a severity that is never absent
/// (a report with no findings still serializes a concrete `"ok"`, never `null`).
#[derive(Serialize)]
struct Rendered<'a> {
    // `severity` first, the schema's own order (`contracts/doctor-report.schema.json`'s
    // `required` list): the emitted object reads the same way the document does.
    severity: Severity,
    findings: &'a [Finding],
}

impl Serialize for Report {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Rendered {
            severity: self.severity(),
            findings: &self.findings,
        }
        .serialize(serializer)
    }
}

impl Report {
    /// The findings, in the order the checks produced them.
    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// Adds a finding.
    pub fn push(&mut self, finding: Finding) {
        self.findings.push(finding);
    }

    /// Adds one leg's findings, in the order it produced them.
    pub fn extend(&mut self, findings: impl IntoIterator<Item = Finding>) {
        self.findings.extend(findings);
    }

    /// The worst severity, `Ok` when there were no findings at all — derived from the findings
    /// rather than cached beside them, so the two cannot disagree.
    pub fn severity(&self) -> Severity {
        self.findings
            .iter()
            .map(|finding| finding.severity)
            .max()
            .unwrap_or(Severity::Ok)
    }

    /// The human rendering: one line per finding, `{severity} {check}: {detail}`, then the
    /// summary naming the worst severity.
    ///
    /// **No trailing newline**, like [`Self::render_json`]: the caller prints the text with
    /// `println!`, which supplies the one line break that ends the output — a `writeln!` here as
    /// well would leave the report a blank line short of its own end.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        for finding in &self.findings {
            // The severity is padded as a string, not as the enum: a derived `Display` writes the
            // word itself and so ignores a width, while `String`'s pads. The column is part of the
            // rendering — the severities line up whatever their length.
            let _ = writeln!(
                out,
                "{:<5} {}: {}",
                finding.severity.to_string(),
                finding.check,
                finding.detail
            );
        }
        let _ = write!(out, "severity: {}", self.severity());
        out
    }

    /// The `--json` rendering of the same report. The only fallible step is the encoding, and a
    /// report that cannot be encoded is an operational failure of this process — not a usage or
    /// storage error — so it becomes [`Error::Operational`] rather than a new `From`.
    pub fn render_json(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(self)
            .map_err(|err| Error::Operational(format!("the report could not be encoded: {err}")))
    }

    /// The process exit code the report's findings earn: `0` for no warn and no error, `1` for
    /// either (the doctor section's exit-code rule). The mode is not an argument here; `--dry-run`/`--fix`
    /// decide which findings the report holds.
    pub fn exit_code(&self) -> u8 {
        match self.severity() {
            Severity::Ok => 0,
            Severity::Warn | Severity::Error => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::cleanup::{RepairAction, RepairActionLevel},
        _util::testing::json_keys,
    };

    fn report() -> Report {
        let mut report = Report::default();
        report.push(Finding::check(
            "storage root",
            Severity::Ok,
            "readable and writable",
        ));
        report
    }

    #[test]
    fn lint_and_info_read_as_ok_and_the_rest_as_themselves() {
        assert_eq!(
            Finding::from_action(action(RepairActionLevel::Lint)).severity,
            Severity::Ok
        );
        assert_eq!(
            Finding::from_action(action(RepairActionLevel::Info)).severity,
            Severity::Ok
        );
        assert_eq!(
            Finding::from_action(action(RepairActionLevel::Warn)).severity,
            Severity::Warn
        );
        assert_eq!(
            Finding::from_action(action(RepairActionLevel::Error)).severity,
            Severity::Error
        );
        // A plain run's proposal row: the same action the `Warn` arm above maps, rendered as
        // information because a "would …" is what a fix *would* change, not a defect yet
        // (Task 2's `Proposals` is the flag that picks this mapping).
        assert_eq!(
            Finding::informational(action(RepairActionLevel::Warn)).severity,
            Severity::Ok
        );
        assert_eq!(
            Finding::informational(action(RepairActionLevel::Warn)).detail,
            Finding::from_action(action(RepairActionLevel::Warn)).detail,
            "the two renderings differ in severity alone"
        );
    }

    fn action(level: RepairActionLevel) -> RepairAction {
        RepairAction {
            level,
            description: "pruned a stale temp file".into(),
        }
    }

    #[test]
    fn the_worst_finding_decides_the_severity() {
        let mut report = report();
        assert_eq!(report.severity(), Severity::Ok);
        report.push(Finding::check(
            "temp files",
            Severity::Warn,
            "3 stale files",
        ));
        assert_eq!(report.severity(), Severity::Warn);
        report.push(Finding::check("meta store", Severity::Error, "unreadable"));
        assert_eq!(report.severity(), Severity::Error);
    }

    #[test]
    fn the_human_rendering_names_every_check_and_its_severity() {
        // The whole rendering, not two `contains`: the finding line, then the summary line.
        let text = report().render_text();
        assert_eq!(
            text.lines().next(),
            Some("ok    storage root: readable and writable"),
            "{text}"
        );
        assert_eq!(text.lines().last(), Some("severity: ok"), "{text}");
        assert_eq!(
            text.lines().count(),
            2,
            "one line per finding, then the summary: {text}"
        );
        // No trailing newline, like the `--json` rendering: the caller prints the text with
        // `println!`, and a `writeln!` here as well would end the report with a blank line.
        assert!(!text.ends_with('\n'), "no trailing newline: {text:?}");
    }

    #[test]
    fn every_severity_has_one_word_in_both_renderings() {
        // One word per variant, in the human rendering and the JSON alike: a rename of a variant
        // or of its `label` that moved one side only fails here (T1-2).
        for (severity, word) in [
            (Severity::Ok, "ok"),
            (Severity::Warn, "warn"),
            (Severity::Error, "error"),
        ] {
            assert_eq!(severity.to_string(), word, "the human word");
            assert_eq!(
                serde_json::to_value(severity).expect("json"),
                serde_json::json!(word),
                "the JSON word"
            );
        }
    }

    #[test]
    fn the_json_rendering_is_the_same_report() {
        let text = report().render_json().expect("json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert_eq!(parsed["findings"][0]["check"], "storage root");
        assert_eq!(parsed["findings"][0]["severity"], "ok");
        assert_eq!(parsed["severity"], "ok");
        // The key set is the contract's, not the implementation's: `doctor-report.schema.json`
        // (Task 4) declares `additionalProperties: false` at both levels, so a field added on
        // one side only must fail here rather than ship as a schema break.
        assert_eq!(json_keys(&parsed), ["findings", "severity"]);
        assert_eq!(
            json_keys(&parsed["findings"][0]),
            ["check", "detail", "severity"]
        );
    }

    #[test]
    fn an_empty_report_renders_a_concrete_severity() {
        let text = Report::default().render_json().expect("json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert_eq!(parsed["findings"], serde_json::json!([]));
        assert_eq!(
            parsed["severity"], "ok",
            "never `null`: a consumer reads one concrete severity"
        );
    }

    #[test]
    fn the_exit_code_follows_the_contract() {
        // The doctor section's exit-code rule: 0 = no problems, 1 = warnings/errors found — with `--dry-run`
        // and with `--fix` alike, where the report holds what the repair left behind. The mode
        // is not an argument to this function: the flags decide which findings the report has
        // (Task 3, through Task 2's `Proposals` — a plain run renders the cleanup proposals as
        // information, `--dry-run` renders them as findings), and the arithmetic is the
        // severity's.
        assert_eq!(report().exit_code(), 0, "0 = no warn and no error");
        let mut dirty = report();
        dirty.push(Finding::check(
            "temp files",
            Severity::Warn,
            "3 stale files",
        ));
        assert_eq!(dirty.exit_code(), 1, "a warn is a problem, in every mode");
        dirty.push(Finding::check("meta store", Severity::Error, "unreadable"));
        assert_eq!(dirty.exit_code(), 1, "so is an error");
    }
}
