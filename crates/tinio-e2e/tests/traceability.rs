//! Spec↔tag traceability cross-check (cucumber BDD migration, task 9).
//!
//! Asserts zero orphans between the spec corpus and the cucumber feature
//! tags, both ways:
//! - every FR/SC/T ID referenced by the spec documents
//!   (`specs/001-s3-local-server/spec.md` +
//!   `{contracts,checklists}/*.md` + `tasks.md`) must appear as a feature
//!   tag, and
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
//! `tasks.md` is unsatisfiable by design. `spec.md` joined the
//! tag-validation side on 2026-10-09 for the same reason — it is the
//! requirement source, so a tag naming one of its IDs is a citation — and
//! it does not join the reverse direction, which stays contracts/ and
//! checklists/ only.
//!
//! The contracts/checklists reverse direction carries a documented
//! allow-list, `NOT_COVERED_BY_CUCUMBER`: IDs whose verification is
//! explicitly outside the cucumber suite (future US2/US3 work that stayed
//! in Rust, memory/performance properties verified by scripts or benches,
//! and citation-only task references). Every entry carries a
//! machine-checked `Home`, so its reason is anchored to something that must
//! still exist whenever this test runs — and an ID that is also a feature
//! tag is a violation, because a tagged ID is already covered and keeping
//! it listed is inert. The check stays strict: any contract/checklist ID
//! that is neither a feature tag nor allow-listed fails the test.

// **A test-support root (docs/style.md §Panics), and a stated one because clippy's test keys do
// not reach it** (design 2026-10-09 D7): `allow-*-in-tests` covers `#[test]` functions and
// `#[cfg(test)]` modules, while this target's helpers (`read_docs`, `collect_md_ids`,
// `collect_feature_tags`) are plain `fn`s in an integration target.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use tinio_util::testing::{walk_files, workspace_root};

/// The runner's default tag filter (Task A1): the platform rule is one
/// function over the target, and the cucumber binary is `harness = false`, so
/// its unit tests ride this default-harness binary. `default_filter` is what the
/// cucumber binary's `main` calls, so nothing here is test-only dead code.
#[cfg(test)]
#[path = "steps/tag_filter.rs"]
mod tag_filter;

/// Where an allow-listed ID's verification actually lives.
enum Home {
    /// A test/bench/script path that must exist on disk.
    Path(&'static str),
    /// An unfinished task in the spec tracker (`- [ ] T0xx` in tasks.md).
    Task(&'static str),
    /// A genuinely non-executable citation; the set is enumerated and asserted closed.
    Meta,
}

/// One allow-list entry: the ID, the reason its verification is outside the
/// cucumber suite, and the home that verification must still have.
///
/// The reason is not free text the checker ignores — a violation message
/// quotes it back, so the entry's claim and the thing that failed to match
/// it are read together.
struct AllowListed {
    /// The spec ID the cucumber cross-check excludes.
    id: &'static str,
    /// Why the ID's verification is outside the cucumber suite.
    reason: &'static str,
    /// Where that verification lives; `check_allow_list` verifies it.
    home: Home,
}

/// The complete `Home::Meta` set: citation-only references with no executable
/// home anywhere. Closed on purpose — a fourth member is a violation — so a
/// new `Meta` entry means changing this constant (a deliberate act), never
/// quietly widening the array.
const META_IDS: &[&str] = &["FR-001", "T010", "T023"];

/// IDs referenced by the spec documents but verified outside the cucumber
/// suite, each with the reason and the machine-checked home of its
/// verification (the s3-surface.md "Automated coverage" section note is the
/// human-facing copy of this argument; this array is the authority where the
/// two disagree).
///
/// **IDs leave this list when scenarios land for them**, the way FR-023 did
/// when the read-only scenarios arrived: FR-007 (the CLI's `server`/`status`/
/// `stop` surface — and `doctor`'s flows, `tests/features/doctor.feature`,
/// which landed after this entry was written: T057's legs are scenarios now),
/// FR-036 (the two sandbox refusals) and SC-005/SC-007 (the readiness and
/// round-trip bounds) left it on 2026-09-22, when the process-level CLI legs
/// became `tests/features/cli_lifecycle.feature` — the same assertions, driven
/// through the real binary from the cucumber suite. FR-016 left it on
/// 2026-10-09 the same way: every level of its chain is a process-surface row
/// now (`confinement.feature`'s precedence rows plus `cli_lifecycle.feature`'s
/// two spawn legs), so the ID is tagged, the mutual-exclusion rule below
/// rejects the entry, and only `.env` *loading* itself — the file read — is
/// still the config layer's, which is T018's entry.
const NOT_COVERED_BY_CUCUMBER: &[AllowListed] = &[
    AllowListed {
        id: "FR-001",
        reason: "meta-requirement (\"serve an S3-compatible interface over \
                 HTTP for a user-chosen local storage root\") — verified by \
                 the suite as a whole, so there is no one test to point at; \
                 cited in checklists/compatibility.md CHK001 only as the \
                 range \"FR-001–FR-015\".",
        home: Home::Meta,
    },
    AllowListed {
        id: "FR-009",
        reason: "anonymous mode's configuration semantics (explicit switch \
                 wins over configured credentials, warning logged) — US3, \
                 CLI/config level, not S3-observable; the no-credentials \
                 request path itself is what the whole in-process suite \
                 exercises (a plane built without a credential pair serves \
                 unsigned requests), while the switch's semantics belong to \
                 T083, the row that cites FR-009 (the flag/env channel is \
                 T087's).",
        home: Home::Task("T083"),
    },
    AllowListed {
        id: "FR-010",
        reason: "bounded-buffer streaming — a memory property, not \
                 observable through the S3 API; the fs backend's \
                 chunk-sequence tests (`get_stream_chunk_sequence`, \
                 `streamed_chunks_own_their_memory`) pin the bounded chunks \
                 the shared body stream serves (`crates/tinio-util/src/\
                 fsutil.rs`), and the end-to-end flat-memory check is the \
                 SC-003 script (T089, open).",
        home: Home::Path("crates/tinio-fs/src/backend/objects.rs"),
    },
    AllowListed {
        id: "FR-017",
        reason: "access logging / OTel export — not S3-observable; verified \
                 by the tinio-server log tests in \
                 `crates/tinio-server/src/log.rs` (T052/T053, both landed).",
        home: Home::Path("crates/tinio-server/src/log.rs"),
    },
    AllowListed {
        id: "FR-018",
        reason: "management plane (/status, /stop, /openapi.json, token) — \
                 US2; verified by the plane's inline Rust suites (T056, \
                 landed): the 2026-10-01 placement cleanup deleted \
                 `crates/tinio-api/tests/management.rs` and moved the legs \
                 beside their subjects in `crates/tinio-api/src/\
                 {transport,router,client,openapi}.rs`.",
        home: Home::Path("crates/tinio-api/src/transport.rs"),
    },
    AllowListed {
        id: "FR-019",
        reason: "metric-recording overhead — a performance property with no \
                 automated verification yet: T092 is open and \
                 `crates/tinio-server/benches/metrics_overhead.rs` is not \
                 written (the bench suite holds pipeline.rs, listing.rs and \
                 multipart_assembly.rs).",
        home: Home::Task("T092"),
    },
    AllowListed {
        id: "FR-035",
        reason: "`tinio service` (install/uninstall/start/stop/query) — US2, \
                 and not S3-observable: the verbs install a service with \
                 systemd, launchd or the SCM, which no cucumber scenario can \
                 hold (a supervisor plus elevation). The pure halves (the \
                 derived label, the install context, the unit text, the \
                 remedy text) are verified by the inline suite in \
                 `crates/tinio-cli/src/service/mod.rs`; the platform \
                 handshake has no auto leg (docs/tests.md, \"Product \
                 surfaces\": \"Service verbs: supervisor + elevation — no \
                 auto leg; logic without elevation: inline unit tests\").",
        home: Home::Path("crates/tinio-cli/src/service/mod.rs"),
    },
    AllowListed {
        id: "SC-003",
        reason: "flat memory on 1 GB transfers — verified by the manual perf \
                 script `e2e/perf/sc003-flat-memory.sh`, which is not written \
                 (the `e2e/perf/` directory does not exist) and whose open \
                 task is T089; cited in checklists/compatibility.md CHK020 \
                 as the flat-memory home.",
        home: Home::Task("T089"),
    },
    AllowListed {
        id: "T010",
        reason: "foundation task; cited in checklists/requirements.md only \
                 as part of the range \"T010–T023\".",
        home: Home::Meta,
    },
    AllowListed {
        id: "T023",
        reason: "foundation task; cited in checklists/requirements.md only \
                 as part of the range \"T010–T023\" (the range's other \
                 endpoint).",
        home: Home::Meta,
    },
    AllowListed {
        id: "T018",
        reason: "`.env` loading — the config layer's, not S3-observable: \
                 T018 landed as `load_env_file` and its tests in the CLI's \
                 composition root at \
                 `crates/tinio-cli/src/config/sources.rs`, the path \
                 contracts/config.md cites for the file's loading.",
        home: Home::Path("crates/tinio-cli/src/config/sources.rs"),
    },
    AllowListed {
        id: "T087",
        reason: "US3's anonymous mode — `TINIO_ANONYMOUS`, the `--anonymous` \
                 flag and the semantics the requirement gives them — is not \
                 implemented in this phase, which contracts/config.md's \
                 2026-09-20 correction states where it records the variable \
                 as unread and names T087 as its owner; T087 owns the wiring \
                 and its tests, so no scenario verifies it. (Entry added \
                 2026-09-22: the correction's citation made the ID \
                 normative.)",
        home: Home::Task("T087"),
    },
    AllowListed {
        id: "T089",
        reason: "the SC-003 flat-memory perf script (manual) — cited in \
                 checklists/compatibility.md CHK020 as the flat-memory \
                 verification home; the perf script itself is not cucumber, \
                 and T089 is the open task that writes it.",
        home: Home::Task("T089"),
    },
    AllowListed {
        id: "T144",
        reason: "the home root-state-dir GC (FR-023's relocation residue) is \
                 not implemented — contracts/cli.md names it as this build's \
                 \"not checked\" leg and `doctor --fix` reports it as not \
                 repaired — and the task is US2's, so no scenario verifies \
                 it.",
        home: Home::Task("T144"),
    },
    AllowListed {
        id: "T146",
        reason: "the debt row contracts/config.md's 2026-09-20 correction \
                 names (the seven `TINIO_ACCESS_LOG*`/`TINIO_API_*` \
                 variables the CLI feature does not honour) — the citation \
                 points at the task that owns the wiring and its unit tests, \
                 so no scenario verifies it.",
        home: Home::Task("T146"),
    },
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

/// Every file of extension `ext` under `dir`, read once, named by its path relative to `dir`.
///
/// The one read behind both corpora below: the documents are the expensive half of this target —
/// the spec corpus is ~769 KB — and each one used to be read once per scan that wanted it.
fn read_docs(dir: &Path, ext: &str) -> Vec<(PathBuf, String)> {
    walk_files(dir, ext)
        .into_iter()
        .map(|path| {
            let rel = path
                .strip_prefix(dir)
                .expect("the walk starts at the corpus root")
                .to_path_buf();
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            (rel, text)
        })
        .collect()
}

/// The spec corpus, read once per process: `spec.md`, `contracts/*.md`, `checklists/*.md` and
/// `tasks.md` under `specs/001-s3-local-server/`, each with its spec-relative path.
///
/// **Three scans read this same corpus under different scopes** — the two ID sets in
/// `spec_ids_and_feature_tags_are_consistent` and the tracker the allow-list's own check reads —
/// so the walk and the read are here rather than in each of them.
static SPEC_DOCS: LazyLock<Vec<(PathBuf, String)>> = LazyLock::new(|| read_docs(&spec_dir(), "md"));

/// The cucumber feature corpus, read once per process — both checks want the same tags.
static FEATURE_DOCS: LazyLock<Vec<(PathBuf, String)>> =
    LazyLock::new(|| read_docs(&features_dir(), "feature"));

/// Every FR/SC/T ID in the `*.md` documents whose corpus-relative path satisfies `in_scope` —
/// the one read-and-extend loop for both spec-ID scans.
fn collect_md_ids(in_scope: impl Fn(&Path) -> bool) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for (rel, text) in SPEC_DOCS.iter() {
        if in_scope(rel) {
            ids.extend(collect_ids(text));
        }
    }
    ids
}

/// Every FR/SC/T ID in the scanned spec documents: `spec.md`,
/// `contracts/*.md`, `checklists/*.md`, and `tasks.md` under
/// `specs/001-s3-local-server/`.
fn collect_spec_ids() -> BTreeSet<String> {
    collect_md_ids(|rel| {
        rel == Path::new("spec.md")
            || rel.starts_with("contracts")
            || rel.starts_with("checklists")
            || rel == Path::new("tasks.md")
    })
}

/// Every FR/SC/T ID in the contracts/ and checklists/ documents — the
/// set the reverse direction (spec ID must be a feature tag) applies to.
fn collect_normative_ids() -> BTreeSet<String> {
    collect_md_ids(|rel| rel.starts_with("contracts") || rel.starts_with("checklists"))
}

/// Every traceability tag in `tests/features/**/*.feature`, out of the corpus read once above.
fn collect_feature_tags() -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for (_, text) in FEATURE_DOCS.iter() {
        tags.extend(collect_tags(text));
    }
    tags
}

/// The tracker's own text, out of the corpus that already read it.
fn tasks_md() -> &'static str {
    SPEC_DOCS
        .iter()
        .find(|(rel, _)| rel.as_path() == Path::new("tasks.md"))
        .expect("the spec corpus holds no tasks.md")
        .1
        .as_str()
}

/// The task IDs of every unchecked row (`- [ ] Txxx`) in a `tasks.md` text.
/// Checked rows (`- [X]`) are excluded on purpose: a done task is never a
/// home, which is what makes `Home::Task` mean "still to be built".
fn collect_unchecked_tasks(text: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("- [ ]") else {
            continue;
        };
        if let Some((id, _)) = parse_id(rest.trim_start(), 0) {
            ids.insert(id);
        }
    }
    ids
}

/// The repository root: this crate lives at `<root>/crates/tinio-e2e`.
///
/// The join and the `env!` are [`tinio_util::testing::workspace_root!`]'s — the macro expands in
/// the crate that calls it, which is what the `env!` needs — and the name stays because five call
/// sites ask for the root rather than for the macro's plumbing.
fn repo_root() -> PathBuf {
    workspace_root!()
}

/// The spec corpus root: `<root>/specs/001-s3-local-server`.
fn spec_dir() -> PathBuf {
    repo_root().join("specs/001-s3-local-server")
}

/// This crate's feature corpus: `<crate>/tests/features`.
fn features_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/features")
}

/// One violation, with the entry's own claim quoted back so the failure says
/// what the entry promised and what it turned out to have.
fn violation(entry: &AllowListed, problem: &str) -> String {
    format!("{}: {problem} [entry reason: {}]", entry.id, entry.reason)
}

/// The allow-list's own cross-check, pure over its inputs: the entries, the
/// feature tags, the tracker's unchecked task IDs, and the repository root a
/// `Path` home resolves against. Every rule is one the audit found broken:
/// a dead `Path`, a `Task` that is not an open row, a `Meta` outside the
/// enumerated bucket, and an ID that is also a feature tag (an entry the
/// suite already covers is inert).
///
/// Returns every violation, never just the first, so one run reports the
/// whole list's rot instead of one round per fix.
fn check_allow_list(
    entries: &[AllowListed],
    tagged: &BTreeSet<String>,
    unchecked_tasks: &BTreeSet<String>,
    repo_root: &Path,
) -> Vec<String> {
    let mut violations = Vec::new();
    for entry in entries {
        if tagged.contains(entry.id) {
            violations.push(violation(
                entry,
                "allow-listed but also a feature tag — a tagged ID is covered, \
                 so the entry is inert",
            ));
        }
        match entry.home {
            Home::Path(path) => {
                if !repo_root.join(path).exists() {
                    violations.push(violation(
                        entry,
                        &format!("home path does not exist: {path}"),
                    ));
                }
            }
            Home::Task(task) => {
                if !unchecked_tasks.contains(task) {
                    violations.push(violation(
                        entry,
                        &format!("home task {task} is not an unchecked `- [ ]` row in tasks.md"),
                    ));
                }
            }
            Home::Meta => {
                if !META_IDS.contains(&entry.id) {
                    violations.push(violation(
                        entry,
                        &format!(
                            "`Meta` is closed to {META_IDS:?}; give the ID a Path or Task home"
                        ),
                    ));
                }
            }
        }
    }
    violations
}

#[test]
fn spec_ids_and_feature_tags_are_consistent() {
    // Tag side: every traceability tag must have a spec ID somewhere in
    // the scanned corpus (spec.md + contracts + checklists + tasks.md).
    let all_ids = collect_spec_ids();
    let tags = collect_feature_tags();
    let orphan_tags: Vec<&String> = tags.iter().filter(|t| !all_ids.contains(*t)).collect();
    assert!(
        orphan_tags.is_empty(),
        "feature tags without a spec ID: {orphan_tags:?}"
    );

    // Spec side: every contract/checklist ID must appear as a feature
    // tag, unless explicitly allow-listed (see NOT_COVERED_BY_CUCUMBER).
    let allow_listed: BTreeSet<&str> = NOT_COVERED_BY_CUCUMBER.iter().map(|e| e.id).collect();
    let normative_ids = collect_normative_ids();
    let missing: Vec<&String> = normative_ids
        .iter()
        .filter(|id| !tags.contains(*id) && !allow_listed.contains(id.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "spec IDs without a feature tag: {missing:?}"
    );
}

/// The allow-list's own health over the real corpus: every home must be live
/// today, and no entry may duplicate a feature tag. This is the check that
/// would have caught the 2026-10-09 audit's four dead entries.
#[test]
fn allow_list_entries_have_live_homes() {
    let root = repo_root();
    let violations = check_allow_list(
        NOT_COVERED_BY_CUCUMBER,
        &collect_feature_tags(),
        &collect_unchecked_tasks(tasks_md()),
        &root,
    );
    assert!(
        violations.is_empty(),
        "allow-list violations: {violations:#?}"
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
    // A backend/classification tag (`@pg`, and the `@fs`/`@mem` it sits beside) is not a spec ID,
    // so it is never collected — which is what "the traceability check accepts `@pg`" means: the
    // `@pg` axis PG17 adds is a scenario-classification tag, not a traceability tag, and a
    // feature carrying one is neither an orphan nor a missing spec ID.
    let text = "@pg @fs @mem @SC-001\nFeature: X\n  @pg\n  Scenario: s\n";
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

/// A one-entry allow-list for the checker's unit cases.
fn one(id: &'static str, home: Home) -> Vec<AllowListed> {
    vec![AllowListed {
        id,
        reason: "unit case",
        home,
    }]
}

#[test]
fn check_allow_list_rejects_a_path_that_does_not_exist() {
    let root = repo_root();
    let tagged = BTreeSet::new();
    let unchecked = BTreeSet::new();
    let missing = one(
        "FR-404",
        Home::Path("crates/tinio-e2e/tests/no-such-file.rs"),
    );
    let violations = check_allow_list(&missing, &tagged, &unchecked, &root);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("no-such-file.rs"), "{violations:?}");
    // The same shape, with a path that is really there, stays clean.
    let present = one(
        "FR-404",
        Home::Path("crates/tinio-e2e/tests/traceability.rs"),
    );
    assert!(
        check_allow_list(&present, &tagged, &unchecked, &root).is_empty(),
        "an existing path must pass"
    );
}

#[test]
fn check_allow_list_rejects_a_task_that_is_not_unchecked() {
    let root = repo_root();
    let tagged = BTreeSet::new();
    let unchecked = BTreeSet::from(["T089".to_string()]);
    // T018's row is closed (`- [X]`) in the tracker: claiming it as a `Task`
    // home is exactly the rot the audit found, and it must fail.
    let closed = one("T018", Home::Task("T018"));
    let violations = check_allow_list(&closed, &tagged, &unchecked, &root);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("T018"), "{violations:?}");
    // An open row is a valid home.
    let open = one("T089", Home::Task("T089"));
    assert!(
        check_allow_list(&open, &tagged, &unchecked, &root).is_empty(),
        "an open task must pass"
    );
}

#[test]
fn check_allow_list_rejects_a_tagged_entry() {
    let root = repo_root();
    let unchecked = BTreeSet::new();
    let tagged = BTreeSet::from(["FR-016".to_string()]);
    // FR-016 is tagged (confinement.feature, cli_lifecycle.feature): a listed
    // ID the scenarios already cover is inert and has to leave the list.
    let listed = one(
        "FR-016",
        Home::Path("crates/tinio-e2e/tests/traceability.rs"),
    );
    let violations = check_allow_list(&listed, &tagged, &unchecked, &root);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("FR-016"), "{violations:?}");
    // An untagged ID with the same home is fine.
    let untagged = one(
        "FR-017",
        Home::Path("crates/tinio-e2e/tests/traceability.rs"),
    );
    assert!(
        check_allow_list(&untagged, &tagged, &unchecked, &root).is_empty(),
        "an untagged entry must pass"
    );
}

#[test]
fn check_allow_list_rejects_a_fourth_meta_home() {
    let root = repo_root();
    let tagged = BTreeSet::new();
    let unchecked = BTreeSet::new();
    let violations = check_allow_list(&one("FR-404", Home::Meta), &tagged, &unchecked, &root);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("Meta"), "{violations:?}");
    // The enumerated members stay admissible, so the bucket can neither grow
    // unnoticed nor quietly shrink.
    for &id in META_IDS {
        assert!(
            check_allow_list(&one(id, Home::Meta), &tagged, &unchecked, &root).is_empty(),
            "{id} must stay admissible"
        );
    }
}

#[test]
fn collect_unchecked_tasks_takes_only_open_rows() {
    let text = "- [ ] T089 [P] Implement the script\n\
                - [X] T018 [P] Implement the resolution\n\
                - [ ] not a task row\n\
                Prose citing T144 outside a row.\n";
    assert_eq!(
        collect_unchecked_tasks(text),
        BTreeSet::from(["T089".to_string()])
    );
}
