//! Design §2's macOS column, measured: the exposed tree is readable, the write
//! grant is exactly the difference between `ReadWrite` and `ReadOnly`, a
//! non-exposed path is refused, deep paths behave, a root exposed through a
//! symlink reads through both its spellings, and a link out of the tree does not.

#![cfg(target_os = "macos")]
// **A macOS-only test target, and a stated exemption because clippy's test keys do not reach
// it** (docs/style.md §Panics, design 2026-10-09 D7): `allow-*-in-tests` covers `#[test]`
// functions and `#[cfg(test)]` modules, while this target's helpers and `harness/` — declared
// below, inheriting this level — are plain `fn`s.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

mod harness;

use std::{fs, os::unix::fs::symlink};

use tinio_sandbox_core::{AccessMode, Sandbox};

/// Runs inside the confinement and reports what each path allows.
#[test]
fn child_exposures() {
    if !harness::is_child_process() {
        harness::launch();
        return;
    }
    let Some(child) = harness::child() else {
        return;
    };
    let mut log = String::new();

    // A path several directories deep, inside the served root, spelled the way
    // the operator spelled the root: proves no ancestor rule is needed for
    // traversal (Seatbelt matches the resolved path, not each component).
    log.push_str(&harness::line("deep_read", read(&child.param("deep"))));
    // The state directory, exposed read-write even alongside a read-only root.
    log.push_str(&harness::line(
        "state_write",
        write(&child.param("state_write")),
    ));
    // The served root: allowed here, denied in the read-only run.
    log.push_str(&harness::line(
        "root_write",
        write(&child.param("root_write")),
    ));
    // A directory the profile does not name, holding a file that does exist —
    // so what is measured is a refusal rather than a missing file.
    log.push_str(&harness::line(
        "sibling_read",
        read(&child.param("sibling")),
    ));
    // The same tree through the root's **canonical** spelling: granted only by
    // the rule `with_canonical_paths` adds, which is the measurement that keeps
    // both spellings in the profile.
    log.push_str(&harness::line(
        "canonical_read",
        read(&child.param("canonical")),
    ));
    // A link inside the exposed tree that points out of it: the kernel resolves
    // it to a path no rule names, so the read is refused.
    log.push_str(&harness::line("escape_read", read(&child.param("escape"))));
    harness::finish(&child, &log);
}

fn read(path: &str) -> String {
    match fs::read_to_string(path) {
        Ok(body) => format!("ok:{}", body.trim()),
        Err(err) => format!("error({})", err.kind()),
    }
}

fn write(path: &str) -> String {
    match fs::write(path, b"written by the confined server\n") {
        Ok(()) => "ok".to_string(),
        Err(err) => format!("error({})", err.kind()),
    }
}

#[test]
fn a_read_write_exposure_is_readable_and_writable() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_exposures` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");

    // **The served root is exposed through a symlink the operator's path goes
    // through**: `link` -> `real`, and the exposure names `link`. That is the
    // case `with_canonical_paths` exists for — Seatbelt compares a rule's
    // spelling against the kernel-resolved path, so `real`'s spelling is what
    // matches — and it is the one measurement that justifies keeping *both*
    // spellings in the profile: `deep` reads through the given one and
    // `canonical` through the resolved one, and the second is granted only by
    // the rule the canonicalisation adds.
    let real = dir.path().join("real");
    let deep = real.join("a/b/c");
    fs::create_dir_all(&deep).expect("create root");
    fs::write(deep.join("obj.txt"), "deep object\n").expect("seed");
    let link = dir.path().join("link");
    symlink(&real, &link).expect("symlink the served root");

    // A link *inside* the exposed tree, pointing out of it.
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).expect("create outside");
    fs::write(outside.join("secret.txt"), "out of the tree\n").expect("seed");
    symlink(&outside, real.join("escape")).expect("symlink out of the tree");

    let sibling = dir.path().join("sibling");
    fs::create_dir_all(&sibling).expect("create sibling");
    fs::write(sibling.join("secret.txt"), "not exposed\n").expect("seed");
    let state = link.join(".tinio");
    fs::create_dir_all(&state).expect("create state");

    let sandbox = Sandbox::new().expose(&link, AccessMode::ReadWrite);
    let report = harness::run_confined(
        &sandbox,
        &[
            ("deep", link.join("a/b/c/obj.txt").display().to_string()),
            ("state_write", state.join("log.txt").display().to_string()),
            ("root_write", link.join("written.txt").display().to_string()),
            ("sibling", sibling.join("secret.txt").display().to_string()),
            (
                "canonical",
                real.join("a/b/c/obj.txt").display().to_string(),
            ),
            (
                "escape",
                link.join("escape/secret.txt").display().to_string(),
            ),
        ],
        "child_exposures",
    );

    report.expect("payload", "ok");
    report.expect("deep_read", "ok:deep object");
    report.expect("state_write", "ok");
    report.expect("root_write", "ok");
    // The two refusals below are asserted as *refusals*, and the expected error
    // is written as a pair on purpose: a read under a directory whose traversal
    // was refused can come back `no such file or directory` rather than
    // `operation not permitted`, and the two are indistinguishable at the
    // syscall boundary. What the test can hold — and what it does hold, with the
    // fixtures on disk — is that the read did not succeed and that the file was
    // there to be read.
    assert!(
        sibling.join("secret.txt").exists(),
        "the fixture must exist"
    );
    assert!(
        matches!(
            report.value("sibling_read"),
            Some("error(permission denied)" | "error(not found)")
        ),
        "{}",
        report.text()
    );
    report.expect("canonical_read", "ok:deep object");
    // The link out of the tree: the tree's rule does not follow it, and the
    // target is named by no rule at all.
    assert!(
        outside.join("secret.txt").exists(),
        "the fixture must exist"
    );
    assert!(
        matches!(
            report.value("escape_read"),
            Some("error(permission denied)" | "error(not found)")
        ),
        "{}",
        report.text()
    );
}

#[test]
fn a_read_only_exposure_refuses_writes_and_still_reads() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_exposures` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    let state = dir.path().join("state");
    let private = dir.path().join("private");
    fs::create_dir_all(&root).expect("create root");
    fs::create_dir_all(&state).expect("create state");
    fs::create_dir_all(&private).expect("create private");
    fs::write(root.join("obj.txt"), "read only\n").expect("seed");
    fs::write(private.join("notes.txt"), "never exposed\n").expect("seed");
    symlink(&private, root.join("escape")).expect("symlink out of the tree");

    // Design §2: the state directory is read-write on every platform, so the
    // report path lives there and the served root stays unwritable.
    let sandbox = Sandbox::new()
        .expose(&root, AccessMode::ReadOnly)
        .expose(&state, AccessMode::ReadWrite);
    let report = harness::run_confined(
        &sandbox,
        &[
            ("deep", root.join("obj.txt").display().to_string()),
            ("state_write", state.join("log.txt").display().to_string()),
            ("root_write", root.join("written.txt").display().to_string()),
            ("sibling", private.join("notes.txt").display().to_string()),
            (
                "canonical",
                fs::canonicalize(root.join("obj.txt"))
                    .unwrap()
                    .display()
                    .to_string(),
            ),
            (
                "escape",
                root.join("escape/notes.txt").display().to_string(),
            ),
        ],
        "child_exposures",
    );

    report.expect("payload", "ok");
    report.expect("deep_read", "ok:read only");
    report.expect("state_write", "ok");
    // A write *inside* an exposed directory is refused with `permission denied`
    // proper — the traversal is granted, so there is no `not found` to confuse
    // it with, which is why this one stays strict.
    assert_eq!(
        report.value("root_write"),
        Some("error(permission denied)"),
        "{}",
        report.text()
    );
    // A read-only root does not make the confinement narrower anywhere else:
    // the unexposed tree is still refused, and the link out of the tree with it.
    assert!(
        matches!(
            report.value("sibling_read"),
            Some("error(permission denied)" | "error(not found)")
        ),
        "{}",
        report.text()
    );
    assert!(
        matches!(
            report.value("escape_read"),
            Some("error(permission denied)" | "error(not found)")
        ),
        "{}",
        report.text()
    );
    report.expect("canonical_read", "ok:read only");
    // The host's copy is untouched too: this is decision 8's "the absence of a
    // write permission", not an application-level check.
    assert!(!root.join("written.txt").exists());
}
