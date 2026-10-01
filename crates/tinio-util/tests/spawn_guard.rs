//! The gate behind `job::spawn`: every spawn site under `crates/*/tests/**` goes through the
//! guard, or is named with its reason in `ALLOWED` below.
//!
//! **The guard is opt-in, and nothing in the type system makes a harness take it.** A harness that
//! writes its own `command.spawn()` gets no job object on Windows and no parent-death signal on
//! Linux, and says nothing about it — which is the hole `job`'s module doc is written around, and
//! the reason its "one spelling" sentence used to be a claim held up by discipline. This gate is
//! what makes it a rule: it walks every `*.rs` file under `crates/*/tests/`, reports every raw
//! site it finds, and fails the workspace test leg on one that `ALLOWED` does not carry.
//!
//! **Why a plain-harness Rust test and not a script.** `docs/tests.md` puts unit and e2e tests in
//! Rust and leaves shell to the platform gate scripts, which are operator-run because they need a
//! host that can confine; this property is platform-neutral and must hold on every leg, which a
//! test in this crate does on all three OSes and in both feature modes — `run-test-leg` runs
//! `--workspace --exclude tinio-e2e` for each of them — with no workflow change at all. It lives
//! in `tinio-util` so that the check and the thing checked are read together, and so the guard's
//! own crate is where a new harness finds it.
//!
//! **What the scan is, exactly: textual, and narrow on purpose.** A site is a `.spawn()` on a code
//! line — the zero-argument method call, which is the spelling `std::process::Command::spawn` has
//! and the one every harness in this tree writes. It is not every `spawn` in sight: `thread::spawn`,
//! `tokio::spawn`, `Scope::spawn` and the harnesses' own wrappers (`PipedChild::spawn(…)`,
//! `Serve::spawn(…)`, `Run::spawn(…)`) all take arguments, so the token cannot match them, and
//! `Type::spawn(…)` is an associated function rather than a method. A `.spawn()` on something that
//! is not a `Command` would be a false positive no text can settle; there is none in the tree, and
//! one would earn an entry below rather than a weaker rule. The one shape the rule would miss is
//! the method spelled as a function — `Command::spawn(&mut cmd)` — which no site in this tree uses
//! and which a reader who writes it should not: it hides the guard from exactly this check.
//!
//! **The scope is the test sources, and the sentence in `job`'s docs says so.** `crates/*/tests/`
//! covers the integration suites, `support/`, `steps/` and the crates' own harness modules, and it
//! covers every one of them without a list: the root manifest's members are `crates/*`, so the
//! roots are read from the filesystem and a new crate's tests are scanned the moment they exist.
//! `#[cfg(test)]` modules inside `src/` are outside it, and one of them is outside it on purpose:
//! `tinio-util`'s own `job.rs` tests spawn raw to prove the guard adopted the test process — a
//! child that went through `job::spawn` could not tell that apart from an assignment after the
//! fact.
//!
//! **A second hole arrived by move rather than by design.** The two relay legs retired
//! `crates/tinio-sandbox-macos/tests/relay.rs` into the inline tests beside the `on_stop` and
//! `disarm` they exercise, so their two `/bin/sh` stand-ins left this scan's scope with them: the
//! placement rule (a unit test lives beside its subject, 2026-10-01) put them in a `#[cfg(test)]`
//! module in `src/`, which this scan does not read by construction. The allowlist entry that used
//! to carry them went with the file, and there is no site left for it to count.
//!
//! **The Windows harness left the same way, and its sites are still guarded.** The placement
//! cleanup promoted `crates/tinio-sandbox-windows/tests/common/` into
//! `crates/tinio-sandbox-windows/src/testing.rs` behind the crate's `testing` feature, so the
//! harness's two spawn sites — `spawn_launcher` and `spawn_unrelayed_child_in_own_group`, both
//! `job::spawn` calls — now sit in `src/` and left this scan's scope with the move. They are
//! **not** allowlist holes: each takes the guard, which is why no entry carries them, and a raw
//! spawn that appears there would earn an entry a scanning gate could no longer see. What the
//! move cost is this gate's sight of them, and this sentence is the record the placement rule
//! asks for (the relay half is the paragraph above).
//!
//! **The hole that sentence anticipated has since arrived, and it is not a `Command` at all.**
//! `tinio-sandbox-windows`'s `testing::low_integrity::run_with` — the device that starts the e2e
//! scenario's Low-integrity child — creates its process with `CreateProcessAsUserW`, because the
//! restricted token and the integrity level it sets are arguments only that call takes. It is
//! therefore neither a `job::spawn` site nor a `.spawn()` one, and this scan could not see it even
//! if it lived under `tests/`. It sits in `src/`, behind that crate's non-default `testing`
//! feature, which is where the placement rule puts a harness device a `tests/` target has to
//! share. **One site**, and it is recorded here rather than in `ALLOWED` because an entry is a
//! permission this gate can check — the count that keeps an entry honest has nothing to count
//! here. What the device gives up by not going through `job::spawn` is the job object that call
//! puts around the child; the `CHILD_DEADLINE_MS` bound it waits under is the device's own.
//!
//! **The allowlist is a list of holes with reasons, not a shelf of exceptions.** Every site that
//! can take the guard takes it; a file is listed only where taking it is impossible or unmeasured,
//! and the count in its entry is what keeps the permission from outliving its site.
//!
//! **A comment is not a site.** The scan reads the code half of each line, because the prose in
//! this tree names the token it looks for (`windows_e2e_harness.rs` does, in the sentence that
//! asks a call site to go through `job::spawn` rather than spell its own). The cut at `//` tracks
//! string literals on the way, so a URL in a string does not hide the rest of its line; it does
//! not track raw strings, which nothing here writes on a line that spawns.

use std::{
    fs,
    path::{Path, PathBuf},
};

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
const ALLOWED: &[(&str, usize, &str)] = &[(
    "crates/tinio-sandbox-macos/tests/harness/mod.rs",
    1,
    "the launcher copy `run_confined` re-execs this test binary under `LAUNCH_ENV` and reads \
         the report it writes; the confined child is a grandchild of *that* copy, so the spawn is \
         the crate's own re-exec chain rather than a child of this process. **The entry is \
         macOS-only, and the guard has nothing to take there**: `job::spawn`'s macOS arm is \
         `Command::spawn` and nothing more, and this host compiles none of this crate \
         (`#![cfg(target_os = \"macos\")]` makes every target here an empty crate off a Mac), so a \
         rewrite would be asserted rather than measured — the thing this workspace's rule about the \
         gated crates forbids. Converting it on a Mac is the follow-up this entry is waiting for.",
)];

#[test]
fn every_harness_spawn_goes_through_the_guard() {
    let root = workspace_root();
    let mut found = Vec::new();
    for tests in scan_roots(&root) {
        for file in rust_files(&tests) {
            let relative = file
                .strip_prefix(&root)
                .expect("a scanned file lives under the workspace root")
                .to_string_lossy()
                .replace('\\', "/");
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            for (line, code) in raw_sites(&text) {
                found.push((relative.clone(), line, code));
            }
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

/// The workspace root, from this test binary's manifest — `crates/tinio-util/Cargo.toml`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the workspace root")
}

/// Every `tests/` directory directly under `crates/`, in a deterministic order.
///
/// **A crate without one is not an error** — most have none, and a crate that grows one belongs in
/// the scan without anyone remembering to add it, which is the whole reason the roots are read
/// from the filesystem rather than listed here.
fn scan_roots(root: &Path) -> Vec<PathBuf> {
    let crates = root.join("crates");
    let entries =
        fs::read_dir(&crates).unwrap_or_else(|e| panic!("cannot read {}: {e}", crates.display()));
    let mut roots: Vec<PathBuf> = entries
        .map(|entry| entry.expect("a crates/ entry").path().join("tests"))
        .filter(|tests| tests.is_dir())
        .collect();
    roots.sort();
    roots
}

/// Every `*.rs` file under `dir`, recursively and in a deterministic order.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries: Vec<_> = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
            .map(|entry| entry.expect("a directory entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                out.push(path);
            }
        }
    }
    out
}

/// The raw spawn sites in `text`: each one's line number and the code it was found on.
fn raw_sites(text: &str) -> Vec<(usize, String)> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            raw_site(code_of(line)).map(|code| (index + 1, code.to_string()))
        })
        .collect()
}

/// The raw spawn site in `code`, if there is one: a `spawn()` whose receiver is a value, which is
/// the `.spawn()` of `std::process::Command` and the token every harness in this tree writes.
fn raw_site(code: &str) -> Option<&str> {
    let mut from = 0;
    while let Some(offset) = code[from..].find("spawn()") {
        let index = from + offset;
        if code[..index].ends_with('.') {
            return Some(code.trim());
        }
        from = index + "spawn()".len();
    }
    None
}

/// `line` with any trailing comment removed — see the module doc for what the cut does and does
/// not track.
fn code_of(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut quoted = false;
    let mut escaped = false;
    let mut index = 0;
    while index + 1 < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if byte == b'/' && bytes[index + 1] == b'/' {
            return &line[..index];
        }
        index += 1;
    }
    line
}
