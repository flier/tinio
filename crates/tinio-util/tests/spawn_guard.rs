//! The gate behind `job::spawn`: every spawn site under `crates/*/tests/**`, and in the test
//! contexts of `crates/*/src` and `xtask/src`, goes through the guard, or is named with its reason
//! in `ALLOWED` below.
//!
//! **The guard is opt-in, and nothing in the type system makes a harness take it.** A harness that
//! writes its own `command.spawn()` gets no job object on Windows and no parent-death signal on
//! Linux, and says nothing about it — which is the hole `job`'s module doc is written around, and
//! the reason its "one spelling" sentence used to be a claim held up by discipline. This gate is
//! what makes it a rule: it parses every `*.rs` file in its scope, reports every raw site it finds,
//! and fails the workspace test leg on one that `ALLOWED` does not carry.
//!
//! **Why a plain-harness Rust test and not a script.** `docs/tests.md` puts unit and e2e tests in
//! Rust and leaves shell to the platform gate scripts, which are operator-run because they need a
//! host that can confine; this property is platform-neutral and must hold on every leg, which a
//! test in this crate does on all three OSes and in both feature modes — `run-test-leg` runs
//! `--workspace --exclude tinio-e2e` for each of them — with no workflow change at all. It lives
//! in `tinio-util` so that the check and the thing checked are read together, and so the guard's
//! own crate is where a new harness finds it.
//!
//! **It builds only with this crate's `testing` feature on**, because the two predicates it walks
//! with are `tinio_util::testing`'s and that module is behind the feature: a `-p tinio-util` leg
//! spells `--features testing`, and the workspace legs (`--workspace`, which is every CI test leg)
//! have it on already, through the dev-dependency edges that request it. A missing feature is a
//! build failure rather than a silent pass, which is the shape this gate wants.
//!
//! **What a site is: three classes, found by a parse.** Each file is parsed with `syn` — one that
//! does not parse fails this test rather than being skipped, because a file this gate cannot read
//! is the one hole it must not have — and the walk reports:
//!
//! - the **zero-argument `.spawn()`** method call, the spelling `std::process::Command::spawn`
//!   has as a method and the one every harness in this tree writes;
//! - the **`…::Command::spawn(…)`** associated-function spelling — the same call written the
//!   other way, which the token scan this replaced could not see at all and a reader could hide
//!   behind;
//! - **`.output()` and `.status()`**, which spawn and wait in one call and are sites for the same
//!   reason: a suite that reaches its server through them never holds the handle the guard's job
//!   membership is about, and the exit is the only thing it ever sees.
//!
//! **The third class is a heuristic, and what it cannot see is a shape rather than an accident.**
//! `.output()`/`.status()` counts when the **receiver subtree builds a `Command`** — `Command::new(…)`
//! itself, the builder calls chained onto it, and the `&`/parens a receiver may wear. A `Command`
//! bound to a name first (`let mut cmd = Command::new("sh"); …; cmd.output()`) is therefore **not**
//! a site: the walk holds no binding table and does not pretend to one. The tree writes both
//! shapes, so the sites this misses were converted one by one in the task that widened the gate and
//! are named in its evidence; a harness that wants its site seen spells the receiver inline.
//!
//! **The first class is wider than `Command`, and cannot be narrowed without types.** A
//! zero-argument `.spawn()` on a type with a `spawn` of its own is a false positive no parse can
//! settle. There is none in the tree, and one would earn an entry below rather than a weaker rule.
//! The wrappers a reader might fear are safe by arity: `PipedChild::spawn(…)`, `Serve::spawn(…)`,
//! `Run::spawn(…)` and `thread::spawn(…)` all take arguments, and `Type::spawn(…)` is an
//! associated function rather than a method. **What the parse cannot see at all is a site written
//! inside a macro invocation's tokens** — `assert!(Command::new(…).spawn().is_ok())` is a token
//! stream to `syn`, not an expression tree — and no site in this tree is spelled that way.
//!
//! **The scope is the test sources, over three roots.** `crates/*/tests/` is test context whole:
//! the integration suites, `support/`, `steps/` and the crates' own harness modules, covered
//! without a list — the root manifest's members are `crates/*`, so the roots are read from the
//! filesystem and a new crate's tests are scanned the moment they exist. `crates/*/src` is the
//! second root and `xtask/src` the third (the one member outside `crates/`, and the same corpus the
//! repo policy test parses), and there **only test context counts**: the walk descends into an item
//! when `tinio_util::testing::is_test_gated` answers true for it — `#[cfg(test)]`,
//! `#[cfg(all(test, …))]`, a `#[test]`-family function, a module declared `#[cfg(feature =
//! "testing")]` — so a production item, the guard's own `Command::spawn` among them, is out of
//! scope by construction. That is the twelve `.spawn()` sites this gate gained: `tinio-util`'s own
//! `job.rs` tests, `tinio-sandbox-core`'s launcher tests, `tinio-sandbox-macos`'s `relay.rs` and
//! `launch.rs`, `tinio-sandbox-windows`'s `sweep.rs`, and `tinio-cli`'s `run.rs`.
//!
//! **Two moves carried sites into `src/`, and only one of them is inside this gate's sight.** The
//! relay half: `crates/tinio-sandbox-macos/src/relay.rs` took the two legs' `/bin/sh` stand-ins
//! from `tests/relay.rs` into the `#[cfg(test)]` module beside the `on_stop` and `disarm` they
//! exercise (a unit test lives beside its subject, 2026-10-01). A `#[cfg(test)]` item is test
//! context, so the widened gate reads them again and `ALLOWED` names them. The Windows half went
//! the other way: `crates/tinio-sandbox-windows/tests/common/` became
//! `crates/tinio-sandbox-windows/src/testing.rs` behind that crate's non-default `testing`
//! feature, and **that gate is declared in `lib.rs`, not in the file** — so the file's own un-gated
//! items read as production items and the walk skips them. The two spawn sites the move was about
//! (`spawn_launcher`, `spawn_unrelayed_child_in_own_group`) take the guard as they always did, and
//! the file's four `.output()`/`.status()` sites were converted in the task that widened this gate
//! rather than left as a hole no count could hold. What stays outside this gate's sight is an
//! un-gated item in a file whose module declaration is the gate, and this sentence is the record
//! the placement rule asks for.
//!
//! **The hole that record anticipated arrived, and it is not a `Command` at all.**
//! `tinio-sandbox-windows`'s `testing::low_integrity::run_with` — the device that starts the e2e
//! scenario's Low-integrity child — creates its process with `CreateProcessAsUserW`, because the
//! restricted token and the integrity level it sets are arguments only that call takes. It is
//! therefore neither a `job::spawn` site nor one of the three classes above, and this scan could
//! not see it even if it lived under `tests/`. **One site**, and it is recorded here rather than in
//! `ALLOWED` because an entry is a permission this gate can check — the count that keeps an entry
//! honest has nothing to count here. What the device gives up by not going through `job::spawn` is
//! the job object that call puts around the child; the `CHILD_DEADLINE_MS` bound it waits under is
//! the device's own.
//!
//! **The allowlist is a list of holes with reasons, not a shelf of exceptions.** Every site that
//! can take the guard takes it, and every site that cannot is named with what it is: the reasons
//! are the site's own where it is deliberately raw (the adoption proof in `job.rs`, the macOS
//! harness's re-exec chain) and the ownership rule that keeps this wave's tasks disjoint where the
//! file belongs to another task. The count in an entry is what keeps the permission from outliving
//! its site.
//!
//! **Prose is not a site.** The walk reads expressions, so the comments in this tree that name the
//! token it looks for — this file's own among them — cannot be sites, and neither can a string
//! literal that spells one. The text scan this replaced needed a `//` cut and a string-literal
//! tracker to reach that answer; a parse has it by construction.

// A test-support root (docs/style.md §Panics), and a stated one because clippy's test
// exemption does not reach it: `allow-*-in-tests` covers `#[test]` functions and
// `#[cfg(test)]` modules, while this target's helpers (`scan_roots`, `scan_files`,
// `raw_sites`) are plain `fn`s in an integration target.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use syn::{
    Expr, ExprCall, ExprMethodCall, Item,
    spanned::Spanned,
    visit::{self, Visit},
};
use tinio_util::testing::{is_test_gated, item_attrs, repo_rel, walk_files, workspace_root};

/// The files whose raw spawns are deliberate, with the number of sites each holds and the reason
/// they are not `job::spawn` sites.
///
/// **A file, not a line, is the unit**, because the reasons are per-file: a suite that must not
/// inherit a job membership of the harness's own is that suite, not one line in it. The count is
/// what keeps the entry honest in both directions — a file that gains a raw spawn fails the check
/// (the count is too low), and an entry left behind by a site that was converted fails it too (the
/// count is too high), so the list cannot rot into a blanket permission.
///
/// **An entry is a hole with a reason, and the reason has to hold.** The bar is `job::spawn`'s own
/// doc: it is the one spelling a harness's spawn sites use. A site leaves this list when it takes
/// the guard, and the check then fails until the entry goes with it — which is the point rather
/// than a nuisance.
///
/// **The reasons are of two kinds, and each is the site's own fact.** Where the site is
/// deliberately raw, the reason is the argument for leaving it raw. Where the file belongs to
/// another task of this wave, the reason is the ownership rule that keeps those tasks disjoint:
/// the site is named here, with what it is, so the count holds it in place until the file's own
/// task converts it — the gate reads every task's files, and only the task that owns a file edits
/// it.
const ALLOWED: &[(&str, usize, &str)] = &[
    (
        "crates/tinio-cli/src/commands/start/run.rs",
        1,
        "the `sh` stand-in child the module tests' `dying` helper spawns. Another task's file in \
         this wave (the ownership rule above), so the site is named here rather than converted.",
    ),
    (
        "crates/tinio-sandbox-core/src/launcher/probe.rs",
        1,
        "the `sh` child the probe tests' `child_writing` helper spawns, whose writes are what that \
         leg reads. Another task's file in this wave (the ownership rule above), so the site is \
         named here rather than converted.",
    ),
    (
        "crates/tinio-sandbox-core/src/launcher/supervise.rs",
        3,
        "the three `sh` stand-ins the supervision tests spawn (`child_for`, `lingering`, \
         `reporting`) — the scripts whose output those legs read the supervisor's own events \
         from. Another task's file in this wave (the ownership rule above), so the sites are named \
         here rather than converted.",
    ),
    (
        "crates/tinio-sandbox-macos/src/launch.rs",
        2,
        "the two `/bin/sleep` stand-ins in the crate's own tests (`a_failed_launch_ends_the_child_\
         it_spawned`, `a_released_child_is_left_running`) — the children those legs observe a \
         launch's arms by. Another task's file in this wave (the ownership rule above), so the \
         sites are named here rather than converted.",
    ),
    (
        "crates/tinio-sandbox-macos/src/relay.rs",
        2,
        "the two `/bin/sh` stand-ins the relay tests signal and disarm. **This is a pair the \
         widened gate brought back into sight**: they left `tests/relay.rs` for the `#[cfg(test)]` \
         module beside the `on_stop` and `disarm` they exercise (a unit test lives beside its \
         subject, 2026-10-01), and a `src` test context is inside this scan's scope now where the \
         `.spawn()`-only text scan's `crates/*/tests/**` root could not read it. Another task's \
         file in this wave (the ownership rule above), so the sites are named here rather than \
         converted.",
    ),
    (
        "crates/tinio-sandbox-macos/tests/harness/mod.rs",
        1,
        "the crate's own re-exec chain: `run_confined` re-execs this test binary as the **launcher \
         copy** under `LAUNCH_ENV` and reads the report the probe writes, so the child this site \
         spawns becomes the confined child's parent — a grandchild of this process, reaped by the \
         harness's own `Children` guard, with the three roles, the argv filter and the report \
         handshake documented around it. **The entry is macOS-only, and the guard has nothing to \
         take there**: `job::spawn`'s macOS arm is `Command::spawn` and nothing more, so a rewrite \
         would spell one no-op call through a function that performs another, on the only platform \
         this file compiles on (`#![cfg(target_os = \"macos\")]` makes every target here an empty \
         crate off a Mac). The crate's other raw sites in the files this task owns — the \
         `.output()`/`.status()` invocations this gate now admits, in `tests/handover.rs` and \
         beside this one, plus the pruning probe's own `sandbox-exec` — are converted in the same \
         task that widened the gate, so what is left here is not an oversight. **This one stays, \
         and what it records is a choice.** A Mac that converts it deletes the entry, and the count \
         check reddens until it does.",
    ),
    (
        "crates/tinio-sandbox-windows/src/acl.rs",
        17,
        "seventeen `icacls(1)` invocations in the crate's own tests — the reads, grants and \
         revokes the ACL fixtures are built from, none of them a server child a job membership \
         would be about. Another task's file in this wave (the ownership rule above), so the sites \
         are named here rather than converted.",
    ),
    (
        "crates/tinio-sandbox-windows/src/sid.rs",
        1,
        "the `icacls` grant in `a_salt_file_that_is_not_this_users_alone_is_refused_rather_than_\
         used` — the file's **test module**, which this task's brief assigns to another one (that \
         task's production half is already converted). Named here rather than converted, so the \
         two halves of one file stay with their owners.",
    ),
    (
        "crates/tinio-sandbox-windows/src/sweep.rs",
        1,
        "the stand-in child the sweep tests' own `spawn` helper creates. Another task's file in \
         this wave (the ownership rule above), so the site is named here rather than converted.",
    ),
    (
        "crates/tinio-util/src/job.rs",
        4,
        "**The adoption proof, deliberately raw, plus the module's other two spawns.** The two \
         `.spawn()` sites are what keeps this crate's own tests honest: a child that went through \
         `job::spawn` could not tell the guard's adoption of an already-running process apart from \
         an assignment after the fact, so the proof has to spell `Command::spawn` itself. The \
         other two are the Linux test module's — the re-exec of this test binary as the parent \
         that aborts without unwinding, and the `kill` that reaps the leftover when the mechanism \
         regresses. `crates/tinio-util/src/job.rs` is outside this task's file list (the guard's \
         own module belongs to another task), so all four are named here rather than converted.",
    ),
    (
        "crates/tinio-winsec/src/dacl.rs",
        2,
        "the two `icacls` grants the DACL tests make (`a_created_file_keeps_an_inheriting_parents_\
         aces_out`, `an_ace_for_another_trustee_is_not_the_current_users_dacl`). Another task's \
         file in this wave (the ownership rule above), so the sites are named here rather than \
         converted.",
    ),
];

#[test]
fn every_harness_spawn_goes_through_the_guard() {
    let root = canonical_root();
    let mut found = Vec::new();
    for (file, in_tests_dir) in scan_files(&root) {
        let relative =
            repo_rel(&root, &file).expect("a scanned file lives under the workspace root");
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
        for (line, code) in raw_sites(&text, in_tests_dir) {
            found.push((relative.clone(), line, code));
        }
    }

    let unexpected: Vec<String> = found
        .iter()
        .filter(|(path, ..)| !ALLOWED.iter().any(|(allowed, ..)| allowed == path))
        .map(|(path, line, code)| format!("{path}:{line}: {code}"))
        .collect();
    assert!(
        unexpected.is_empty(),
        "a harness spawns without the guard — `job::spawn(&mut command)` is the one spelling, and \
         a site that genuinely must not take it belongs in `ALLOWED` with its reason:\n{}",
        unexpected.join("\n")
    );

    // The other direction, so the allowlist cannot rot: an entry whose file holds fewer sites
    // than it claims is a site that was converted and an entry that was not — the permission a
    // reader would still find here is for a spawn that is gone.
    for (path, sites, why) in ALLOWED {
        let actual = found.iter().filter(|(found, ..)| found == path).count();
        assert_eq!(
            sites, &actual,
            "{path} is allowlisted for {sites} raw spawn(s) — {why}\n\
             the scan found {actual}: a site was added or converted without this entry moving"
        );
    }
}

/// The scan's three classes, its scope rule and the guard's own spelling, on sources small enough
/// to read: the probe that keeps the definition above honest when the walk is edited.
#[test]
fn the_scan_knows_its_three_classes_and_its_scope() {
    let in_scope = r#"
fn f() {
    let _ = std::process::Command::new("x").spawn();
    let _ = std::process::Command::spawn(&mut cmd);
    let _ = std::process::Command::new("x").output();
    // a comment mentioning .spawn()
    let _ = job::spawn(&mut cmd);
}
"#;
    assert_eq!(
        raw_sites(in_scope, true).len(),
        3,
        "{:?}",
        raw_sites(in_scope, true)
    );
    let guarded = "fn f() { let mut c = job::spawn(&mut cmd)?; let _ = c.status()?; }";
    assert!(raw_sites(guarded, true).is_empty());
    let scoped = "fn prod() { let _ = Command::new(\"x\").output(); }\n#[cfg(test)]\nmod t { fn t() { let _ = Command::new(\"x\").spawn(); } }";
    assert_eq!(
        raw_sites(scoped, false).len(),
        1,
        "{:?}",
        raw_sites(scoped, false)
    );
}

/// The rest of the site definition: the associated spellings of all three classes, the wrappers
/// that take arguments and so cannot be one, and the two shapes that are prose rather than code.
#[test]
fn the_scan_admits_the_associated_spellings_and_neither_the_guard_nor_prose() {
    let associated = r#"
fn f() {
    let _ = std::process::Command::output(&mut cmd);
    let _ = Command::status(&mut cmd);
}
"#;
    assert_eq!(
        raw_sites(associated, true).len(),
        2,
        "{:?}",
        raw_sites(associated, true)
    );

    let not_sites = r#"
fn f() {
    let _ = job::spawn(&mut cmd);
    let _ = PipedChild::spawn(piped);
    let _ = thread::spawn(|| {});
    // a comment naming `.output()` and `Command::spawn`
    let text = ".spawn()";
}
"#;
    assert!(
        raw_sites(not_sites, true).is_empty(),
        "{:?}",
        raw_sites(not_sites, true)
    );
}

/// **The shape the third class cannot see, asserted rather than left to a reader.** A `Command`
/// bound to a name before `.output()` is not a site — the walk holds no binding table — and a
/// probe that pins the hole is what makes the doc's sentence about it checkable. The sites the
/// tree writes that way were converted by hand in the task that widened this gate, because this
/// scan cannot count them.
#[test]
fn the_scan_cannot_see_a_command_bound_before_it_is_used() {
    let bound_first = r#"
fn f() {
    let mut cmd = std::process::Command::new("sh");
    cmd.arg("-c").arg("true");
    let _ = cmd.output();
}
"#;
    assert!(
        raw_sites(bound_first, true).is_empty(),
        "{:?}",
        raw_sites(bound_first, true)
    );
}

/// The scope rule's arms on a `src` file: the `#[cfg(feature = "testing")]` module and the
/// `#[cfg(all(test, …))]` module are test context, and the production item beside them is not.
#[test]
fn the_scan_reads_srcs_test_context_and_skips_srcs_production() {
    let source = r#"
pub fn production() {
    let _ = Command::new("x").output();
}

#[cfg(feature = "testing")]
mod harness {
    pub fn helper() {
        let _ = Command::new("x").status();
    }
}

#[cfg(all(test, unix))]
mod tests {
    #[test]
    fn a_leg() {
        let _ = Command::new("x").spawn();
    }
}
"#;
    assert_eq!(
        raw_sites(source, false).len(),
        2,
        "{:?}",
        raw_sites(source, false)
    );
}

/// **All three roots are read, and the one outside `crates/` is not forgotten.** The walk's
/// coverage is the other half of the site definition — a class nothing walks is not a gate — and a
/// root that quietly dropped out would show up here rather than as a green run over fewer files.
#[test]
fn the_scan_reads_the_three_roots() {
    let root = canonical_root();
    let files: Vec<String> = scan_files(&root)
        .into_iter()
        .map(|(file, _)| {
            repo_rel(&root, &file).expect("a scanned file lives under the workspace root")
        })
        .collect();
    for expected in [
        "xtask/src/main.rs",
        "crates/tinio-util/tests/spawn_guard.rs",
        "crates/tinio-util/src/lib.rs",
    ] {
        assert!(
            files.iter().any(|file| file == expected),
            "`{expected}` is not in the scanned set ({} files)",
            files.len()
        );
    }
}

/// The canonical workspace root — the shared resolver's answer, with the `..` in it resolved and
/// the path checked to exist, which is what lets the assertions below compare two real paths.
///
/// **The `env!` half is `workspace_root!`'s**, which expands in *this* crate: the macro is the one
/// spelling of it, and the caller stays the one that reads it — which is what the wrapper adds to
/// the two call sites below, the canonicalisation a comparison of two real paths needs.
fn canonical_root() -> PathBuf {
    workspace_root!()
        .canonicalize()
        .expect("the workspace root")
}

/// Every `tests/` (or `src/`) directory directly under `crates/`, in a deterministic order.
///
/// **A crate without one is not an error** — most have none, and a crate that grows one belongs in
/// the scan without anyone remembering to add it, which is the whole reason the roots are read
/// from the filesystem rather than listed here.
fn scan_roots(root: &Path, name: &str) -> Vec<PathBuf> {
    let crates = root.join("crates");
    let entries =
        fs::read_dir(&crates).unwrap_or_else(|e| panic!("cannot read {}: {e}", crates.display()));
    let mut roots: Vec<PathBuf> = entries
        .map(|entry| entry.expect("a crates/ entry").path().join(name))
        .filter(|dir| dir.is_dir())
        .collect();
    roots.sort();
    roots
}

/// Every file the gate reads, with whether it sits under a `tests/` directory.
///
/// **Three roots, and the third is why this is a list of files and not of test trees.**
/// `crates/*/tests` is test context whole; `crates/*/src` is production except where a
/// `#[cfg(test)]` item says otherwise; `xtask/src` joins the second group because it is the one
/// workspace member outside `crates/` and the repo policy test parses the same corpus
/// (`crates/*/src` + `xtask/src`). Which of the two the walker answers is the flag beside each
/// path, and it is the whole difference [`raw_sites`] is handed.
fn scan_files(root: &Path) -> Vec<(PathBuf, bool)> {
    let mut out = Vec::new();
    for tests in scan_roots(root, "tests") {
        out.extend(
            walk_files(&tests, "rs")
                .into_iter()
                .map(|file| (file, true)),
        );
    }
    let mut sources = scan_roots(root, "src");
    let xtask = root.join("xtask").join("src");
    if xtask.is_dir() {
        sources.push(xtask);
    }
    for src in sources {
        out.extend(walk_files(&src, "rs").into_iter().map(|file| (file, false)));
    }
    out
}

/// The raw spawn sites in `text`, by line number with the code found there.
///
/// Raw spawn sites: zero-arg `.spawn()`, the `…::Command::spawn(…)` spelling, and
/// `.output()`/`.status()` whose receiver subtree builds a `Command` (documented heuristic:
/// `let cmd = Command::new(..); cmd.output()` is missed).
fn raw_sites(text: &str, in_tests_dir: bool) -> Vec<(usize, String)> {
    let file = syn::parse_file(text).unwrap_or_else(|e| panic!("a scanned file must parse: {e}"));
    let mut scan = SpawnScan {
        in_tests_dir,
        in_test: false,
        lines: Vec::new(),
    };
    scan.visit_file(&file);
    let mut lines = scan.lines;
    lines.sort_unstable();
    lines.dedup();
    lines
        .into_iter()
        .map(|line| {
            let code = text
                .lines()
                .nth(line - 1)
                .unwrap_or_default()
                .trim()
                .to_string();
            (line, code)
        })
        .collect()
}

/// Whether `path`'s last-but-one segment is `Command` — the one predicate the two classes that
/// **spell the type** share.
///
/// `Command::new(…)` is what a `.output()`/`.status()` receiver is built from, and
/// `…::Command::spawn(…)` is the associated spelling of all three sites; both need the same
/// question answered, and the gate's value is that they agree on one answer rather than each
/// slicing `segments` its own way. Written over the reversed iterator so the arity rule is one
/// thing (`nth(1)` is `None` for a single-segment path, which is what the old `n >= 2` was).
fn command_path(path: &syn::Path) -> bool {
    path.segments
        .iter()
        .rev()
        .nth(1)
        .is_some_and(|s| s.ident == "Command")
}

/// The walk that finds the sites: it carries the scope rule down the tree and records a line for
/// each site it passes.
struct SpawnScan {
    in_tests_dir: bool,
    in_test: bool,
    lines: Vec<usize>,
}

impl SpawnScan {
    /// Whether `expr` builds a `std::process::Command` — `Command::new(…)`, through the builder
    /// calls a harness chains onto it and through the `&`/`(…)` wrappers a receiver may wear.
    fn command_receiver(expr: &Expr) -> bool {
        match expr {
            Expr::Call(c) => matches!(&*c.func, Expr::Path(p)
                if p.path.segments.last().is_some_and(|s| s.ident == "new")
                    && command_path(&p.path)),
            Expr::MethodCall(m) => Self::command_receiver(&m.receiver),
            Expr::Paren(p) => Self::command_receiver(&p.expr),
            Expr::Reference(r) => Self::command_receiver(&r.expr),
            _ => false,
        }
    }
}

impl<'ast> Visit<'ast> for SpawnScan {
    /// The scope rule, applied one item at a time: a production item in `src/` is outside the
    /// gate, and nothing under it is read; a test-gated item is inside it, and everything under it
    /// is read whether it looks gated or not.
    fn visit_item(&mut self, item: &'ast Item) {
        let gated = is_test_gated(item_attrs(item));
        if !self.in_tests_dir && !self.in_test && !gated {
            return; // production item in `src/` — outside the gate's scope
        }
        let prev = self.in_test;
        self.in_test |= gated;
        visit::visit_item(self, item);
        self.in_test = prev;
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let method = call.method.to_string();
        let hit = (method == "spawn" && call.args.is_empty())
            || (matches!(method.as_str(), "output" | "status")
                && Self::command_receiver(&call.receiver));
        if hit {
            self.lines.push(call.method.span().start().line);
        }
        visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let Expr::Path(p) = &*call.func
            && command_path(&p.path)
            && p.path.segments.last().is_some_and(|s| {
                let member = &s.ident;
                member == "spawn" || member == "output" || member == "status"
            })
        {
            self.lines.push(p.path.span().start().line);
        }
        visit::visit_expr_call(self, call);
    }
}
