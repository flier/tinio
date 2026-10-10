//! Repo policy gate (design 2026-10-09 D8): production code carries no unannotated
//! assert family, no `#[allow]` escape from the panic lints, and only prefixed
//! `#[expect]` reasons.
//!
//! The scan is a `syn` parse, not a text match, and every production file must
//! parse: a parse failure is itself a violation, so the gate can never silently
//! skip a file it cannot read. Test contexts are skipped by attribute
//! (`tinio_util::testing::is_test_gated` — the one rule, shared with the spawn
//! guard); an `#[allow]` on a `mod` declaration that resolves to a test-support
//! file is the D7 exception and passes.
//!
//! The lint-level attributes are checked in exactly one place,
//! `visit_attribute`, so every position is covered — a file's inner `#![…]`
//! (a whole-file escape) as much as an item's, an impl item's, a statement's or
//! a local's.
//!
//! Documented blind spot: a local macro that expands to `assert!` is invisible to
//! this parse, and `macro_rules!` bodies are token streams the visitor never
//! descends into (`debug_assert!` is out of rule — D4).

use std::{collections::HashSet, fs, sync::LazyLock};

use syn::{
    Attribute, ExprMacro, Item, Macro, Meta, StmtMacro, Token,
    punctuated::Punctuated,
    spanned::Spanned,
    visit::{self, Visit},
};
use tinio_util::testing::{is_test_gated, item_attrs, repo_rel, walk_files, workspace_root};

const LINT_NAMES: &[&str] = &[
    "unwrap_used",
    "expect_used",
    "panic",
    "todo",
    "unimplemented",
    "unreachable",
];
const REASON_PREFIXES: &[&str] = &["literal:", "infallible:", "guarded:", "argued:"];

/// The one sentence a file that does not parse is reported with: the gate must see every file it
/// is pointed at, so a parse failure is a violation rather than a skip.
fn does_not_parse() -> &'static str {
    "does not parse — fix the file or the scan"
}

/// Violations in one production file, parsed from its text (`rel` is repo-relative for
/// messages): the entry point the probes below use, where [`scan_file`] is the one the corpus
/// takes.
fn scan_source(rel: &str, text: &str) -> Vec<String> {
    match syn::parse_file(text) {
        Ok(file) => scan_file(rel, &file),
        Err(_) => vec![format!("{rel}: {}", does_not_parse())],
    }
}

/// Violations in one production file of the corpus, over a parse already paid for.
fn scan_file(rel: &str, file: &syn::File) -> Vec<String> {
    let mut scan = Scan {
        rel: rel.to_string(),
        dir: rel
            .rsplit_once('/')
            .map(|(d, _)| d.to_string())
            .unwrap_or_default(),
        out: Vec::new(),
        allow_exempt: false,
    };
    scan.visit_file(file);
    scan.out
}

struct Scan {
    rel: String,
    dir: String,
    out: Vec<String>,
    /// Set while the attributes of a `mod name;` declaration that resolves to a
    /// test-support file are visited — the one `#[allow]` the D7 exception
    /// admits. `visit_attribute` consumes it.
    allow_exempt: bool,
}

impl Scan {
    /// The one lint-level attribute this gate owns (D8): `#[allow]` escapes and
    /// `#[expect]` reasons. Every attribute position reaches it through
    /// `visit_attribute` — a file's inner `#![…]` included, which a per-node
    /// walk would miss.
    fn check_attr(&mut self, a: &Attribute) {
        let is_allow = a.path().is_ident("allow");
        let is_expect = a.path().is_ident("expect");
        if !(is_allow || is_expect) {
            return;
        }
        let Ok(metas) = a.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
            return;
        };
        let lint = metas.iter().find_map(|m| match m {
            Meta::Path(p) => p.segments.last().and_then(|s| {
                let name = s.ident.to_string();
                LINT_NAMES.contains(&name.as_str()).then_some(name)
            }),
            _ => None,
        });
        let Some(lint) = lint else { return };
        let line = a.span().start().line;
        if is_allow {
            if self.allow_exempt {
                return;
            }
            self.out.push(format!(
                "{}:{line}: `#[allow(clippy::{lint})]` escapes the gate — use `#[expect]` with a reason",
                self.rel
            ));
            return;
        }
        let reason = metas.iter().find_map(|m| match m {
            Meta::NameValue(nv) if nv.path.is_ident("reason") => match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) => Some(s.value()),
                _ => None,
            },
            _ => None,
        });
        let ok = reason
            .as_deref()
            .is_some_and(|r| REASON_PREFIXES.iter().any(|p| r.starts_with(p)));
        if !ok {
            self.out.push(format!(
                "{}:{line}: `#[expect(clippy::{lint})]` reason missing or unprefixed (D2)",
                self.rel
            ));
        }
    }

    /// `debug_assert*` is out of rule (D4) — only the three assert macros count.
    fn macro_invocation(&mut self, mac: &Macro) {
        let Some(seg) = mac.path.segments.last() else {
            return;
        };
        let name = seg.ident.to_string();
        if matches!(name.as_str(), "assert" | "assert_eq" | "assert_ne") {
            let line = mac.path.span().start().line;
            self.out.push(format!(
                "{}:{line}: assert family in production (`{name}!`) — return a `Result` (D1)",
                self.rel
            ));
        }
    }

    /// Does a `mod name;` declaration in this file resolve to an exempt file?
    fn resolves_to_exempt(&self, name: &str) -> bool {
        is_test_support_module(&format!("{}/{name}.rs", self.dir))
            || is_test_support_module(&format!("{}/{name}/mod.rs", self.dir))
    }
}

impl<'ast> Visit<'ast> for Scan {
    /// The only override that is not a check: a test-gated item is skipped
    /// whole (a nested `mod tests` included), and a `mod name;` declaration
    /// that resolves to a test-support file arms the D7 exception for the
    /// attributes `visit_attribute` is about to see.
    fn visit_item(&mut self, item: &'ast Item) {
        let attrs = item_attrs(item);
        if is_test_gated(attrs) {
            return;
        }
        let exempt = match item {
            Item::Mod(m) if m.content.is_none() => self.resolves_to_exempt(&m.ident.to_string()),
            _ => false,
        };
        let prev = self.allow_exempt;
        self.allow_exempt = exempt;
        visit::visit_item(self, item);
        self.allow_exempt = prev;
    }

    /// The single check point: a file's inner `#![…]`, an item's, an impl
    /// item's, a statement's and a local's attributes all pass through here.
    fn visit_attribute(&mut self, attr: &'ast Attribute) {
        self.check_attr(attr);
        visit::visit_attribute(self, attr);
    }

    fn visit_stmt_macro(&mut self, m: &'ast StmtMacro) {
        self.macro_invocation(&m.mac);
        visit::visit_stmt_macro(self, m);
    }

    fn visit_expr_macro(&mut self, m: &'ast ExprMacro) {
        self.macro_invocation(&m.mac);
        visit::visit_expr_macro(self, m);
    }
}

// `is_test_gated` / `item_attrs` are defined once in
// `crates/tinio-util/src/testing/mod.rs` (step 4b) and imported above — the
// spawn guard (Task A5) consumes the same ones, and so do `workspace_root`
// (through its `workspace_root!` macro, which reads this crate's own
// `CARGO_MANIFEST_DIR`), `repo_rel` and `walk_files`, which is where the walk
// this file used to carry now lives. What stays local is the scope:
// `crates/*/src` + `xtask/src` and the `/tests/` `/benches/` exclusions below —
// and [`CORPUS`], the one pass over that scope both tests read.

/// Test-support modules that compile outside `cfg(test)` (D7): each carries a
/// stated allow in its crate instead of a cleanup.
fn is_test_support_module(rel: &str) -> bool {
    const EXEMPT: &[&str] = &[
        "crates/tinio-util/src/bench.rs",
        "crates/tinio-util/src/job.rs",
        "crates/tinio-util/src/testing/mod.rs",
        "crates/tinio-util/src/testing/metadata.rs",
        "crates/tinio-blob/src/testing.rs",
        "crates/tinio-fs/src/testing.rs",
        "crates/tinio-sandbox-windows/src/testing.rs",
        "crates/tinio-sandbox-linux/src/testing.rs",
        "crates/tinio-sandbox-linux/src/bin/sandbox-child.rs",
    ];
    EXEMPT.contains(&rel)
}

/// The two answers both tests read, built from one walk, one read and one parse per file.
struct Corpus {
    /// Files that are test context through their parent — [`test_module_files`]'s answer.
    test_modules: HashSet<String>,
    /// The production scan's violations, in walk order, each already named for a message.
    violations: Vec<String>,
}

/// The one pass over the corpus both tests read: every `*.rs` file under `crates/` and `xtask/`,
/// read and parsed once per process, answered twice.
///
/// **One walk and one parse where there used to be three of each.** The two tests read the same
/// tree — the production scan used to walk and parse it a second time after the classification
/// had — so both answers come out of this one loop, and the parses live only inside it (a
/// `syn::File` is not `Sync`, so a shared static cannot hold them). The scope stays each pass's
/// own: the classification reads every file whole, and the production scan skips parts of the
/// same set.
///
/// **A file that does not parse is a violation, never a skip** — that is the production scan's
/// rule — **but it declares no `mod`s to the classification**, which is what the classification
/// always did with one. `crates/tinio-e2e/**` and `benches/**` are read by the classification for
/// the same reason: it reads them even though the production scan skips them.
///
/// **Two phases, and the order matters**: a test module is named by its *parent's* declaration
/// (`resolve/mod.rs` makes `resolve/tests.rs` one), and the parent is not always walked first, so
/// the classification has to finish before the production scan can ask what it found.
///
/// # Panics
///
/// When a file under `crates/` or `xtask/` cannot be read, or the walk names one outside the
/// root. A corpus the gate cannot see whole is the one hole it must not have — the same rule the
/// module doc states for a file that does not parse — so this fails rather than skipping.
#[expect(
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a corpus file the walk cannot name or read is the gate failing loudly, not a library error path (the hole the parse-failure rule closes), and this builder is a plain fn in an integration target, which clippy's test exemption does not reach (docs/style.md §Panics)"
)]
fn build_corpus() -> Corpus {
    let root = workspace_root!();
    let mut parsed = Vec::new();
    let mut test_modules = HashSet::new();
    for dir in ["crates", "xtask"] {
        for path in walk_files(&root.join(dir), "rs") {
            let rel = repo_rel(&root, &path).expect("walk starts at the repo root");
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            let file = syn::parse_file(&text).ok();
            if let Some(file) = &file {
                let parent = rel.rsplit_once('/').map_or("", |(d, _)| d);
                for item in &file.items {
                    let Item::Mod(m) = item else { continue };
                    if m.content.is_some() || !is_test_gated(&m.attrs) {
                        continue;
                    }
                    let name = m.ident.to_string();
                    for candidate in mod_file_candidates(parent, &name, &m.attrs) {
                        if root.join(&candidate).is_file() {
                            test_modules.insert(candidate);
                        }
                    }
                }
            }
            parsed.push((rel, file));
        }
    }

    let mut violations = Vec::new();
    for (rel, file) in &parsed {
        if rel.contains("/tests/")
            || rel.contains("/benches/")
            || rel.starts_with("crates/tinio-e2e")
            || test_modules.contains(rel)
            || is_test_support_module(rel)
        {
            continue;
        }
        match file {
            Some(file) => violations.extend(
                scan_file(rel, file)
                    .into_iter()
                    .map(|v| format!("{rel}: {v}")),
            ),
            None => violations.push(format!("{rel}: {}", does_not_parse())),
        }
    }
    Corpus {
        test_modules,
        violations,
    }
}

static CORPUS: LazyLock<Corpus> = LazyLock::new(build_corpus);

/// Files that are test context through their parent: `#[cfg(test)] mod tests;`
/// in `resolve/mod.rs` makes `resolve/tests.rs` a test module, and the module
/// file carries no attribute of its own. The walk goes file by file, so the
/// parent's gate is read in its own pass rather than missed.
///
/// Memoised over [`CORPUS`]: both tests ask the same question of the same tree, and answering it
/// reads every item of every file.
fn test_module_files() -> &'static HashSet<String> {
    &CORPUS.test_modules
}

/// The files a `mod name;` declaration can resolve to, `#[path = "…"]` first.
///
/// **`#[path]` is not cosmetic, and the ident is not always the file name.** `store.rs` declares
/// `#[cfg(test)] #[path = "store_tests.rs"] mod tests;`: the module's file is named by the
/// attribute, so the `name.rs`/`name/mod.rs` pair looks for a `src/tests.rs` that does not exist
/// and the file is scanned as production instead of being skipped as the test module it is. The
/// value is relative to the directory of the file carrying the declaration, which is why `parent`
/// is joined in front of it.
fn mod_file_candidates(parent: &str, name: &str, attrs: &[Attribute]) -> Vec<String> {
    match mod_path(attrs) {
        Some(path) => vec![format!("{parent}/{path}")],
        None => vec![
            format!("{parent}/{name}.rs"),
            format!("{parent}/{name}/mod.rs"),
        ],
    }
}

/// A `mod` declaration's `#[path = "…"]` value, if it carries one.
fn mod_path(attrs: &[Attribute]) -> Option<String> {
    attrs.iter().find_map(|a| {
        let Meta::NameValue(nv) = &a.meta else {
            return None;
        };
        if !a.path().is_ident("path") {
            return None;
        }
        match &nv.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => Some(s.value()),
            _ => None,
        }
    })
}

#[test]
fn production_assert_is_red() {
    assert_eq!(scan_source("t", "fn f() { assert!(x); }").len(), 1);
}

#[test]
fn cfg_test_module_is_green() {
    assert!(scan_source("t", "#[cfg(test)]\nmod tests { fn t() { assert!(x); } }").is_empty());
}

#[test]
fn cfg_all_test_unix_module_is_green() {
    assert!(
        scan_source(
            "t",
            "#[cfg(all(test, unix))]\nmod tests { fn t() { assert!(x); } }"
        )
        .is_empty()
    );
}

#[test]
fn bare_test_fn_is_green() {
    assert!(scan_source("t", "#[test]\nfn t() { assert!(x); }").is_empty());
}

#[test]
fn macro_body_and_debug_assert_are_green() {
    assert!(scan_source("t", "macro_rules! m { () => { assert!(true); } }").is_empty());
    assert!(scan_source("t", "fn f() { debug_assert!(x); }").is_empty());
}

#[test]
fn bare_allow_is_red_and_prefixed_expect_is_green() {
    assert_eq!(
        scan_source("t", "#[allow(clippy::unwrap_used)]\nfn f() {}").len(),
        1
    );
    assert!(
        scan_source(
            "t",
            "#[expect(clippy::unwrap_used, reason = \"literal: fixed\")]\nfn f() {}"
        )
        .is_empty()
    );
    assert_eq!(
        scan_source(
            "t",
            "#[expect(clippy::unwrap_used, reason = \"because\")]\nfn f() {}"
        )
        .len(),
        1
    );
}

#[test]
fn file_level_allow_is_red() {
    assert_eq!(
        scan_source("t", "#![allow(clippy::unwrap_used)]\nfn f() {}").len(),
        1
    );
}

#[test]
fn test_support_mod_allow_is_green() {
    assert!(
        scan_source(
            "crates/tinio-fs/src/lib.rs",
            "#[allow(clippy::unwrap_used)]\npub mod testing;"
        )
        .is_empty()
    );
}

#[test]
fn path_attributed_test_modules_are_green() {
    let modules = test_module_files();
    for rel in [
        "crates/tinio-store-pg/src/store_tests.rs",
        "crates/tinio-store-pg/src/store_db_tests.rs",
        "crates/tinio-store-pg/src/content_tests.rs",
    ] {
        assert!(
            modules.contains(rel),
            "`#[path]`-declared test module is not classified as one: {rel} \
             ({} files classified)",
            modules.len()
        );
    }
}

#[test]
fn workspace_is_clean() {
    assert!(
        CORPUS.violations.is_empty(),
        "policy violations:\n{}",
        CORPUS.violations.join("\n")
    );
}
