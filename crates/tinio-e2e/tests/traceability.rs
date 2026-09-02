//! Spec↔tag traceability cross-check (cucumber BDD migration, task 9).
//!
//! Asserts zero orphans between the spec corpus and the cucumber feature
//! tags, both ways:
//! - every FR/SC/T ID referenced by the spec documents
//!   (`specs/001-s3-local-server/{contracts,checklists}/*.md` + `tasks.md`)
//!   must appear as a feature tag, and
//! - every traceability tag in `tests/features/**/*.feature` must have a
//!   spec ID.
//!
//! # Scoping ruling (T9-A, 2026-09-01, recorded in the task-9 report)
//!
//! `tasks.md` contributes its T0xx/FR/SC IDs to the tag-validation side
//! only. The reverse direction (every spec ID must be a feature tag)
//! applies to `contracts/` and `checklists/` — the spec-semantic
//! documents. `tasks.md` is the implementation task tracker: its ~100
//! implementation-task IDs (T001–T023, T036–T103) have no feature
//! semantics, and tagging features with them would be incorrect (the
//! plan's "correct IDs only" rule). A naive bidirectional scan over
//! `tasks.md` is unsatisfiable by design.
//!
//! The contracts/checklists reverse direction carries a documented
//! allow-list, `NOT_COVERED_BY_CUCUMBER`: IDs whose verification is
//! explicitly outside the cucumber suite (future US2/US3 work that stayed
//! in Rust, memory/performance properties verified by scripts or benches,
//! and citation-only task references). The check stays strict: any
//! contract/checklist ID that is neither a feature tag nor allow-listed
//! fails the test.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

/// IDs referenced by the spec documents but verified outside the cucumber
/// suite, each with the reason (mirrored in the s3-surface.md "Automated
/// coverage" section note).
const NOT_COVERED_BY_CUCUMBER: &[&str] = &[
    // FR-001: meta-requirement ("serve an S3-compatible interface over
    // HTTP for a user-chosen local storage root") — verified by the suite
    // as a whole; cited in checklists/compatibility.md CHK001 only as the
    // range "FR-001–FR-015".
    "FR-001",
    // FR-007: CLI subcommands (server/status/stop/doctor) — US2; verified
    // by the tinio-cli lifecycle/doctor tests that stayed in Rust
    // (tasks.md T055/T057).
    "FR-007",
    // FR-009: anonymous mode's configuration semantics (explicit switch
    // wins over credentials, warning logged) — US3, CLI/config level, not
    // S3-observable; the anonymous-request path itself is what the whole
    // in-process suite exercises (unsigned requests succeed).
    "FR-009",
    // FR-010: bounded-buffer streaming — a memory property, not
    // observable through the S3 API; verified by the SC-003 flat-memory
    // script (T089) and the Rust streaming-mechanics tests.
    "FR-010",
    // FR-016: configuration precedence / `.env` loading — not
    // S3-observable; verified by the tinio-config tests (T018).
    "FR-016",
    // FR-017: access logging / OTel export — not S3-observable; verified
    // by the tinio-server log tests (T052/T053).
    "FR-017",
    // FR-018: management plane (/status /stop /openapi.json, token) — US2;
    // verified by the tinio-api tests that stayed in Rust (T056).
    "FR-018",
    // FR-019: metric-recording overhead — a performance property;
    // verified by the metrics_overhead bench (T092).
    "FR-019",
    // FR-023: read-only mode — US2, not yet implemented (T058 unchecked);
    // nothing to exercise.
    "FR-023",
    // SC-003: flat memory on 1 GB transfers — verified by the manual
    // perf script (T089).
    "SC-003",
    // SC-005: readiness within 1 s — US2 timing criterion (T094), not
    // observable in the in-process suite.
    "SC-005",
    // SC-007: status/stop round-trip within 1 s — US2 timing criterion
    // (T094).
    "SC-007",
    // T010/T023: foundation tasks; cited in checklists/requirements.md
    // only as part of the range "T010–T023".
    "T010", "T023",
    // T018: `.env` loading — config layer; cited in contracts/config.md;
    // not S3-observable (the FR-016 entry above).
    "T018",
    // T089: the SC-003 flat-memory perf script (manual) — cited in
    // checklists/compatibility.md CHK020 as the flat-memory verification
    // home; the perf script itself is not cucumber.
    "T089",
];

/// Parses a spec ID at `text[i..]`: prefix `FR`/`SC`/`T`, an optional
/// hyphen, exactly three digits, and a trailing non-alphanumeric byte (or
/// EOF) — matching `\b(FR|SC|T)-?\d{3}\b`. Returns the ID and the offset
/// just past it.
fn parse_id(text: &str, i: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    if i >= bytes.len() {
        return None;
    }
    let prefix_len = if text[i..].starts_with("FR") || text[i..].starts_with("SC") {
        2
    } else if bytes[i] == b'T' {
        1
    } else {
        return None;
    };
    // Word boundary before the prefix: the previous byte must not be
    // alphanumeric (so "AT025" or "ST025" never match).
    if i > 0 && bytes[i - 1].is_ascii_alphanumeric() {
        return None;
    }
    let mut j = i + prefix_len;
    if bytes.get(j) == Some(&b'-') {
        j += 1;
    }
    if j + 3 > bytes.len() || !bytes[j..j + 3].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let end = j + 3;
    // Word boundary after the digits ("T003a" and "T025x" must not match).
    if end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
        return None;
    }
    Some((text[i..end].to_string(), end))
}

/// Collects every spec ID in `text` (`FR-xxx` / `SC-xxx` / `Txxx` shapes).
fn collect_ids(text: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let mut i = 0;
    while i < text.len() {
        if let Some((id, end)) = parse_id(text, i) {
            ids.insert(id);
            i = end;
        } else {
            // Advance one char (not one byte): a multi-byte char must
            // never leave `i` inside a char boundary.
            i += text[i..].chars().next().map_or(1, char::len_utf8);
        }
    }
    ids
}

/// Collects the `@(FR|SC|T)-?\d{3}` tags in a feature file's text.
/// `#`-prefixed comment lines are prose, not tags — a header comment
/// mentioning an ID (e.g. "(T032, T034)" citations) must never count.
fn collect_tags(text: &str) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for line in text.lines() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        let bytes = line.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'@'
                && let Some((id, _)) = parse_id(line, i + 1)
            {
                tags.insert(id);
            }
        }
    }
    tags
}

/// All files with extension `ext` under `dir`, recursively (deterministic
/// order) — one walker for the `*.md` spec corpus and the `*.feature`
/// files.
fn walk(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries: Vec<_> = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == ext) {
                out.push(path);
            }
        }
    }
    out
}

/// Every FR/SC/T ID in the `*.md` documents whose relative path satisfies
/// `in_scope` — the one read-and-extend loop for both spec-ID scans.
fn collect_md_ids(dir: &Path, in_scope: impl Fn(&Path) -> bool) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for path in walk(dir, "md") {
        let rel = path.strip_prefix(dir).unwrap();
        if in_scope(rel) {
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            ids.extend(collect_ids(&text));
        }
    }
    ids
}

/// Every FR/SC/T ID in the scanned spec documents: `contracts/*.md`,
/// `checklists/*.md`, and `tasks.md` under `specs/001-s3-local-server/`.
fn collect_spec_ids(spec_dir: &Path) -> BTreeSet<String> {
    collect_md_ids(spec_dir, |rel| {
        rel.starts_with("contracts")
            || rel.starts_with("checklists")
            || rel == std::path::Path::new("tasks.md")
    })
}

/// Every FR/SC/T ID in the contracts/ and checklists/ documents — the
/// set the reverse direction (spec ID must be a feature tag) applies to.
fn collect_normative_ids(spec_dir: &Path) -> BTreeSet<String> {
    collect_md_ids(spec_dir, |rel| {
        rel.starts_with("contracts") || rel.starts_with("checklists")
    })
}

/// Every traceability tag in `tests/features/**/*.feature`.
fn collect_feature_tags(features_dir: &Path) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for path in walk(features_dir, "feature") {
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        tags.extend(collect_tags(&text));
    }
    tags
}

#[test]
fn spec_ids_and_feature_tags_are_consistent() {
    let spec_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../specs/001-s3-local-server");
    let features_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/features");

    // Tag side: every traceability tag must have a spec ID somewhere in
    // the scanned corpus (contracts + checklists + tasks.md).
    let all_ids = collect_spec_ids(&spec_dir);
    let tags = collect_feature_tags(&features_dir);
    let orphan_tags: Vec<&String> = tags.iter().filter(|t| !all_ids.contains(*t)).collect();
    assert!(
        orphan_tags.is_empty(),
        "feature tags without a spec ID: {orphan_tags:?}"
    );

    // Spec side: every contract/checklist ID must appear as a feature
    // tag, unless explicitly allow-listed (see NOT_COVERED_BY_CUCUMBER).
    let normative_ids = collect_normative_ids(&spec_dir);
    let missing: Vec<&String> = normative_ids
        .iter()
        .filter(|id| !tags.contains(*id) && !NOT_COVERED_BY_CUCUMBER.contains(&id.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "spec IDs without a feature tag: {missing:?}"
    );
}

#[test]
fn parse_id_matches_the_spec_id_shapes() {
    // FR-xxx and SC-xxx carry a hyphen in the contracts/checklists, Txxx
    // does not in tasks.md; both must parse.
    for (text, expected) in [
        ("FR-002", "FR-002"),
        ("SC-004", "SC-004"),
        ("T025", "T025"),
        ("(FR-020;", "FR-020"),
        ("T032-2026", "T032"),
        ("FR-001–FR-015", "FR-001"),
    ] {
        let i = text.find(['F', 'S', 'T']).unwrap_or(0);
        assert_eq!(
            parse_id(text, i).map(|(id, _)| id),
            Some(expected.to_string()),
            "parse_id({text:?})"
        );
    }
    // Non-matches: suffixed ids (T003a), embedded prefixes, non-digit
    // tails, and truncated ids.
    for text in ["T003a", "AT025", "ST025", "FR-0x2", "T0xx", "FR-02", "T"] {
        assert!(parse_id(text, 0).is_none(), "parse_id({text:?})");
    }
    // A range citation yields both endpoints.
    let ids = collect_ids("Spec §FR-001–FR-015");
    assert_eq!(
        ids,
        BTreeSet::from(["FR-001".to_string(), "FR-015".to_string()])
    );
}

#[test]
fn collect_tags_takes_only_at_prefixed_ids() {
    // Header-comment references (no @ prefix) must not count as tags.
    let text = concat!(
        "@SC-001\nFeature: X\n  @interop @aws @SC-002\n  Scenario: s\n",
        "# replaces e2e/interop/journey.sh (T032, T034)\n"
    );
    assert_eq!(
        collect_tags(text),
        BTreeSet::from(["SC-001".to_string(), "SC-002".to_string()])
    );
    // An @-prefixed ID inside a comment line is prose too.
    let text = "# @FR-099 is cited, not tagged\n@SC-001\nFeature: X\n";
    assert_eq!(collect_tags(text), BTreeSet::from(["SC-001".to_string()]));
}

#[test]
fn parse_id_at_eof_is_a_clean_miss() {
    // A feature file ending in a bare `@` (no ID after it) must be a
    // parse miss, not an out-of-bounds panic.
    assert!(parse_id("@", 1).is_none());
    assert!(parse_id("  @", 3).is_none());
    assert_eq!(
        collect_tags("@SC-001\nFeature: X\n  @"),
        BTreeSet::from(["SC-001".to_string()])
    );
}
