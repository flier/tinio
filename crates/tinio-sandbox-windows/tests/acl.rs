//! The DACL whitelist against a real filesystem, with `icacls` as the
//! independent oracle for the grant, the rollback and the inheritance answer
//! design §6 settled on 2026-09-19.
//!
//! **Every test here writes real ACEs**, and only ever onto a temporary directory
//! it created itself: the operator's tree is not this suite's to edit, and a
//! failure must not be able to leave a residue outside the process's own temp
//! tree.

#![cfg(target_os = "windows")]
// `Grants::new` takes an `Arc<PackageSid>` — this plan's interface, and the shape
// the launcher itself holds: one SID shared with the launch rather than a copy per
// user. `PackageSid` owns a raw `PSID`, so it is deliberately neither `Send` nor
// `Sync`, and clippy's lint about that is about an `Arc` meant to cross a thread
// boundary. This one is shared ownership inside one test, which is what the code
// under test is given.
#![allow(clippy::arc_with_non_send_sync)]

mod common;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

// The oracle is the harness's, and this binary declares `mod common` for it: the
// same checked-exit-status `icacls` every suite that asserts on the DACL text
// uses, so the rule that makes it an oracle is written once.
use common::icacls;
use tinio_sandbox_core::AccessMode;
use tinio_sandbox_windows::{
    acl,
    acl::Target,
    backend::SiblingScan,
    sid::{PackageSid, package_name},
};

/// The SID this run derives for `root` — the same question the launcher asks.
///
/// **Called once per name, and the value held.** `PackageSid` owns the
/// AppContainer profile behind the SID and deletes it when the value drops
/// (decision 21), so a second value for one name would delete the profile the
/// first is still using — measured on 2026-09-21: `DeleteAppContainerProfile`
/// answers a profile that is already gone with `S_OK`, so the second delete is
/// silent, and what it leaves behind is a live `PackageSid` whose profile no
/// longer exists, which cannot launch (decision 21's measurement of 2026-09-19).
fn sid_for(root: &Path) -> PackageSid {
    PackageSid::derive(&package_name(root)).expect("derive the package SID")
}

/// The ACE lines of `icacls`'s rendering of `path`.
///
/// One entry per ACE, with the path `icacls` prefixes the first line with
/// removed and its own count line dropped.
///
/// The point of extracting them is that **a comparison of these is independent of
/// how any ACE renders**. That matters for the foreign-ACE assertion below:
/// `icacls` prints a well-known SID by its account name (`BUILTIN\Users…`, and
/// the name is localised), so searching the text for `S-1-5-32-545` would fail on
/// every host.
fn ace_lines(rendered: &str, path: &Path) -> Vec<String> {
    let prefix = path.to_string_lossy().to_string();
    rendered
        .lines()
        .map(str::trim)
        .filter(|line| line.contains(':'))
        .map(|line| {
            line.strip_prefix(prefix.as_str())
                .unwrap_or(line)
                .trim()
                .to_string()
        })
        .collect()
}

/// The answer design §6 settled on 2026-09-19, pinned as a regression test.
///
/// The question it settles was whether an inheritable ACE on a directory reaches
/// children that were already there when it was set: every probe behind the old
/// evidence table used `icacls … /t`, which applies the ACE to each child
/// explicitly, so inheritance alone was never isolated. Measured on NTFS (`C:`)
/// and ReFS (`E:`) alike, a directory-only `/grant:r` — **no `/t`** — does reach
/// them, as an inherited `(I)` entry, and a directory-only `/remove:g` takes every
/// copy back. The grant list is therefore one ACE per exposed directory, there is
/// no `/t` walk, and this test is what fails if that stops being true on the
/// filesystem this crate is serving from.
///
/// **What the tool counts and what the filesystem does are two different
/// counts.** `icacls` was handed one path and says so; the propagation is the
/// filesystem's, and it is also the cost — both passes touch every descendant the
/// served tree holds (Task 13 measures it).
#[test]
fn the_directory_ace_alone_reaches_a_file_that_was_already_there() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    let depth_one = root.join("direct.txt");
    let nested = root.join("bucket").join("nested.txt");
    fs::create_dir_all(nested.parent().expect("the bucket")).expect("create the tree");
    fs::write(&depth_one, b"before the grant").expect("seed");
    fs::write(&nested, b"before the grant").expect("seed");
    // One value, alive for the whole test: it owns the profile the grants are for.
    let sid = Arc::new(sid_for(&root));
    let sid_text = sid.as_str().to_string();

    assert!(
        !acl::has_ace(&depth_one, &sid).expect("read the DACL"),
        "no package ACE yet"
    );

    let mut grants = acl::Grants::new(Arc::clone(&sid));
    grants
        .apply(&root, Target::Directory, AccessMode::ReadWrite)
        .expect("grant");

    for file in [&depth_one, &nested] {
        let before = icacls(file, &[]);
        assert!(
            before.contains(&sid_text),
            "an inheritable ACE on the directory did not reach {}, which was there before the\n\
             grant — so the grant list would need the `/t` walk design §6 rules out, and the\n\
             per-file rollback that comes with it. File DACL:\n{before}",
            file.display()
        );
        assert!(
            before.to_lowercase().contains("inherited") || before.contains("(I)"),
            "the entry on the pre-existing file should be the *inherited* one, not a second \
             explicit ACE:\n{before}"
        );
    }

    // A file created afterwards is the case the design's rows already covered; it
    // must keep working either way.
    let created_later = root.join("bucket").join("after.txt");
    fs::write(&created_later, b"after the grant").expect("write");
    assert!(acl::has_ace(&created_later, &sid).expect("read the DACL"));

    grants.rollback().expect("roll back");
    // Read the DACL text itself rather than asking our own predicate: an assertion
    // that rests on `has_ace` alone would also pass for a path whose DACL could not
    // be read, and the point of the rollback is the operator's directory, not this
    // crate's view of it.
    for path in [&root, &depth_one, &nested, &created_later] {
        assert!(
            !icacls(path, &[]).contains(&sid_text),
            "the package ACE is gone from the whole tree: {}",
            path.display()
        );
    }
    // And the predicate agrees with the oracle, which is what lets the suites that
    // have no oracle of their own rely on it.
    assert!(!acl::has_ace(&root, &sid).expect("read the DACL"));
}

#[test]
fn a_grant_leaves_a_foreign_ace_alone_and_the_rollback_removes_exactly_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("create the root");
    let plain = ace_lines(&icacls(&root, &[]), &root);

    // A foreign ACE the run must not touch, in the shape the verified row
    // describes: `Users` granted on the directory. Named by SID so this does not
    // depend on how the host spells that account's name.
    let granted = Command::new(acl::icacls_binary())
        .arg(&root)
        .args(["/grant", "*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users");
    assert!(
        granted.status.success(),
        "{}",
        String::from_utf8_lossy(&granted.stderr)
    );

    let before = ace_lines(&icacls(&root, &[]), &root);
    assert_eq!(
        before.len(),
        plain.len() + 1,
        "the foreign ACE was written, so the comparison below is about a real one: {before:?}"
    );

    let sid = Arc::new(sid_for(&root));
    let sid_text = sid.as_str().to_string();
    let mut grants = acl::Grants::new(Arc::clone(&sid));
    grants
        .apply(&root, Target::Directory, AccessMode::ReadWrite)
        .expect("grant");
    assert!(
        acl::has_ace(&root, &sid).expect("read the DACL"),
        "the grant landed"
    );

    grants.rollback().expect("roll back");

    let after = ace_lines(&icacls(&root, &[]), &root);
    assert!(
        !after.iter().any(|line| line.contains(&sid_text)),
        "the package ACE is gone: {after:?}"
    );
    // The half that does not depend on how a SID renders, which is the half the
    // first draft of this test got wrong: `icacls` prints `S-1-5-32-545` as
    // `BUILTIN\Users` (and the name is localised), so the property decision 15
    // states — the rollback removed exactly the ACEs it added and left every other
    // one as it found it — is asserted as an equality of the ACE lines rather than
    // as a search for a string the tool never prints.
    assert_eq!(
        before, after,
        "the rollback removed exactly one ACE and left every other ACE as it found it"
    );
}

#[test]
fn a_rollback_with_nothing_granted_is_a_no_op() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut grants = acl::Grants::new(Arc::new(sid_for(dir.path())));
    assert!(grants.is_empty());
    grants.rollback().expect("an empty rollback succeeds");
}

#[test]
fn revoking_an_absent_ace_succeeds() {
    // Idempotence is what lets the rollback run over paths whose grant never
    // landed, and what makes a crashed run's residue removable twice. Measured on
    // this host 2026-09-21: `/remove:g` for a SID with no entry anywhere under the
    // path exits 0 — "Successfully processed 1 files; Failed processing 0 files" —
    // so this is the tool's behaviour rather than a kindness this crate adds.
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = sid_for(dir.path());
    acl::revoke(dir.path(), &sid).expect("first revoke");
    acl::revoke(dir.path(), &sid).expect("second revoke");
}

#[test]
fn dropping_the_grants_removes_them() {
    // The guard is the path a panic takes; a leaked ACE there would outlive the
    // process.
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = Arc::new(sid_for(dir.path()));
    {
        let mut grants = acl::Grants::new(Arc::clone(&sid));
        grants
            .apply(dir.path(), Target::Directory, AccessMode::ReadOnly)
            .expect("grant");
        assert!(acl::has_ace(dir.path(), &sid).expect("read the DACL"));
    }
    // The oracle, not the predicate: this assertion is about the operator's
    // directory, which is the thing the guard is supposed to have restored.
    assert!(!icacls(dir.path(), &[]).contains(sid.as_str()));
}

#[test]
fn the_predicate_refuses_a_path_it_cannot_read() {
    // `has_ace` is the oracle the rest of this phase's suites consult, and an
    // oracle that answered "no entry for this SID" for a path `icacls` refused to
    // read would make every "the ACE is gone" assertion pass for the wrong reason.
    // Measured on this host 2026-09-21: a path that does not exist makes `icacls`
    // exit 2 with the reason on stderr.
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("never-created");
    let sid = sid_for(dir.path());

    let err = acl::has_ace(&missing, &sid).expect_err("a path icacls cannot read is an error");
    let text = err.to_string();
    assert!(text.contains("icacls"), "{text}");
    assert!(
        text.contains("never-created"),
        "the failure names the path it was about: {text}"
    );
}

#[test]
fn a_rollback_finishes_even_when_one_path_fails() {
    // "A failure does not stop the loop" is what makes the residue story complete:
    // a rollback that returned at its first error would leave every later path's
    // ACEs on the operator's disk. The first path to be revoked here — the last one
    // granted, since the loop runs in reverse — is removed after its grant, so its
    // revoke is the failure and the other path's revoke is what proves the loop
    // carried on.
    let dir = tempfile::tempdir().expect("temp dir");
    let kept = dir.path().join("kept");
    let gone = dir.path().join("gone");
    fs::create_dir_all(&kept).expect("create the tree");
    fs::create_dir_all(&gone).expect("create the tree");
    let sid = Arc::new(sid_for(dir.path()));
    let mut grants = acl::Grants::new(Arc::clone(&sid));
    grants
        .apply(&kept, Target::Directory, AccessMode::ReadOnly)
        .expect("grant");
    grants
        .apply(&gone, Target::Directory, AccessMode::ReadOnly)
        .expect("grant");
    fs::remove_dir_all(&gone).expect("remove a granted directory");

    let err = grants
        .rollback()
        .expect_err("revoking a path that is gone fails");
    assert!(
        err.to_string().contains("gone"),
        "the reported failure is the path that failed: {err}"
    );
    assert!(grants.is_empty(), "every path was attempted and forgotten");
    assert!(
        !acl::has_ace(&kept, &sid).expect("read the DACL"),
        "the path after the failure was rolled back anyway"
    );
}

#[test]
fn a_grant_that_failed_is_not_recorded_for_rollback() {
    // Decision 15 rolls back the grants this run applied. A path whose grant never
    // landed must not be on that list: the rollback after a failed startup would
    // otherwise report failures for entries it never wrote, and the first one it
    // kept would hide the real one.
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("never-created");
    let sid = Arc::new(sid_for(dir.path()));
    let mut grants = acl::Grants::new(Arc::clone(&sid));

    assert!(
        grants
            .apply(&missing, Target::Directory, AccessMode::ReadOnly)
            .is_err(),
        "a grant on a path icacls cannot see fails"
    );
    assert!(grants.is_empty(), "the failed grant was not recorded");
    grants.rollback().expect("nothing to take back");
}

/// **The file-shaped spec is what actually lands on a file, and the inherited
/// form is the silent no-op the design's evidence row records** — decision 22's
/// rule is the first time this crate puts *files* into an exposure set, so the
/// two halves of this test are one measurement: what the grant has to be, and
/// what it must not be.
///
/// The inherited form is run first and asserted to write **nothing**, because
/// that is the failure the counter-shape exists to prevent and it is silent:
/// `icacls` exits 0 and reports success, so a rule that shipped the directory
/// spec for the sibling DLLs would grant nothing and show up as a confined child
/// that dies `0xC0000135` with no diagnostic (design decision 22).
#[test]
fn the_file_shaped_spec_is_what_lands_on_a_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let sibling = dir.path().join("sibling.dll");
    fs::write(&sibling, b"not really a library").expect("write the sibling");
    let sid = Arc::new(sid_for(dir.path()));

    assert!(
        !acl::has_ace(&sibling, &sid).expect("read the DACL"),
        "no package ACE yet"
    );

    // The directory shape, on a file. The invocation itself succeeds — the tool
    // exits 0 and reports one file processed — and the ACE is nowhere: this is
    // the no-op, not an error. A well-known SID is used rather than a derived
    // package SID because this half is a fact about the tool and the filesystem
    // rather than about this crate's SID, and it needs no AppContainer profile.
    let written = dir.path().join("written.txt");
    fs::write(&written, b"a plain file").expect("write the control");
    let users = "S-1-5-32-545";
    let inherited = Command::new(acl::icacls_binary())
        .arg(&written)
        .args(["/grant:r", &format!("*{users}:(OI)(CI)M"), "/q"])
        .output()
        .expect("run icacls");
    assert!(
        inherited.status.success(),
        "the inherited shape is accepted on a file: {}",
        String::from_utf8_lossy(&inherited.stderr)
    );
    assert!(
        !icacls(&written, &[]).contains(users),
        "an inheritance-flagged grant on a file wrote an ACE after all, so the \
         file-shaped spec below is not what makes a sibling grant land and this \
         crate's reason for it needs re-measuring (design §6's evidence row): {}",
        icacls(&written, &[])
    );

    // The file shape: no `(OI)`/`(CI)`, so the entry is explicit and stays. The
    // path probe is the crate's own, so this half is the shape this crate
    // actually writes for the sibling DLLs.
    acl::grant(&sibling, Target::File, AccessMode::ReadOnly, &sid)
        .expect("the file shape is accepted on a file");
    assert!(
        acl::has_ace(&sibling, &sid).expect("read the DACL"),
        "the file-shaped grant is what puts an ACE on a file: {}",
        icacls(&sibling, &[])
    );

    acl::revoke(&sibling, &sid).expect("take the grant back");
    assert!(!acl::has_ace(&sibling, &sid).expect("read the DACL"));
}

/// **The sibling set goes on through one invocation and comes off through one**
/// (Ruling 78, decision 22). The directory's pattern is the grant the launcher
/// actually makes, and every sibling it covers is asserted through the oracle
/// rather than through one path: a batch that granted only the first match would
/// leave a confined child that dies `0xC0000135` with no diagnostic, which is the
/// failure the counter-shape exists to prevent.
#[test]
fn the_sibling_batch_grants_and_rolls_back_every_dll() {
    let dir = tempfile::tempdir().expect("temp dir");
    let siblings: Vec<PathBuf> = ["one.dll", "two.dll", "three.dll"]
        .iter()
        .map(|name| {
            let path = dir.path().join(name);
            fs::write(&path, b"not really a library").expect("write the sibling");
            path
        })
        .collect();
    let sid = Arc::new(sid_for(dir.path()));

    let mut grants = acl::Grants::new(Arc::clone(&sid));
    assert!(
        grants
            .apply_siblings(&SiblingScan::scan(dir.path()), AccessMode::ReadOnly)
            .expect("one batched grant"),
        "the directory holds siblings, so the grant ran"
    );
    for sibling in &siblings {
        assert!(
            acl::has_ace(sibling, &sid).expect("read the DACL"),
            "the batch granted {}: {}",
            sibling.display(),
            icacls(sibling, &[])
        );
    }

    grants.rollback().expect("one batched rollback");
    for sibling in &siblings {
        assert!(
            !icacls(sibling, &[]).contains(sid.as_str()),
            "the batch removed the ACE from {}",
            sibling.display()
        );
    }
    assert!(grants.is_empty());
}

/// A directory with no sibling is a grant of nothing: no `icacls` invocation, no
/// recorded path, and a rollback that has nothing to take back. **An unmatched
/// pattern is an error rather than a no-op** (measured 2026-09-21), so this is the
/// negative that keeps a grant on a DLL-less directory from reporting success.
#[test]
fn a_directory_with_no_sibling_grants_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(dir.path().join("notes.txt"), b"not a library").expect("write a non-DLL");
    let sid = Arc::new(sid_for(dir.path()));

    let mut grants = acl::Grants::new(Arc::clone(&sid));
    assert!(
        !grants
            .apply_siblings(&SiblingScan::scan(dir.path()), AccessMode::ReadOnly)
            .expect("nothing to grant"),
        "no sibling means no grant"
    );
    assert!(grants.is_empty(), "and nothing recorded for rollback");
    grants.rollback().expect("nothing to take back");
}
