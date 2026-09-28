//! The DACL whitelist against a real filesystem, with `icacls` as the
//! independent oracle for the grant, the standing skip and the inheritance answer
//! design §6 settled on 2026-09-19.
//!
//! **Every test here writes real ACEs**, and only ever onto a temporary directory
//! it created itself: the operator's tree is not this suite's to edit, and a
//! failure must not be able to leave a residue outside the process's own temp
//! tree.
//!
//! **Nothing the launcher does takes an ACE back any more.** Every grant is
//! standing (`acl::Grants`'s doc states why and what it gives up), so the tests
//! that used to assert a rollback now assert the opposite — the ACE is still
//! there after the guard drops — and `acl::revoke` survives as the primitive an
//! operator (or a test) removes one with.

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
    sid::{ALL_APPLICATION_PACKAGES, PackageSid, package_name},
    testing,
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
/// them, as an inherited `(I)` entry. The grant list is therefore one ACE per
/// exposed directory, there is no `/t` walk, and this test is what fails if that
/// stops being true on the filesystem this crate is serving from.
///
/// **What the tool counts and what the filesystem does are two different
/// counts.** `icacls` was handed one path and says so; the propagation is the
/// filesystem's, and it is also the cost — one descriptor write walks every
/// descendant the served tree holds, which is why the grants are standing and why
/// the launcher reads before it writes (`acl::grant_to`'s skip).
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

    {
        let mut grants = acl::Grants::new(Arc::clone(&sid));
        grants
            .apply(&root, Target::Directory, AccessMode::ReadWrite)
            .expect("grant");
    }

    for file in [&depth_one, &nested] {
        let after = icacls(file, &[]);
        assert!(
            after.contains(&sid_text),
            "an inheritable ACE on the directory did not reach {}, which was there before the\n\
             grant — so the grant list would need the `/t` walk design §6 rules out. File DACL:\n{after}",
            file.display()
        );
        assert!(
            after.to_lowercase().contains("inherited") || after.contains("(I)"),
            "the entry on the pre-existing file should be the *inherited* one, not a second \
             explicit ACE:\n{after}"
        );
    }

    // A file created afterwards is the case the design's rows already covered; it
    // must keep working either way.
    let created_later = root.join("bucket").join("after.txt");
    fs::write(&created_later, b"after the grant").expect("write");
    assert!(acl::has_ace(&created_later, &sid).expect("read the DACL"));

    // **The guard is gone and the ACE is not**, which is the standing contract:
    // `Grants` owns the profile's lifetime and nothing else. Read the DACL text
    // itself rather than only asking our own predicate: an assertion that rests on
    // `has_ace` alone would also pass for a path whose DACL could not be read, and
    // the point is the operator's directory, not this crate's view of it.
    for path in [&root, &depth_one, &nested, &created_later] {
        assert!(
            icacls(path, &[]).contains(&sid_text),
            "the package ACE is still on the whole tree after the guard dropped: {}",
            path.display()
        );
    }
    // And the predicate agrees with the oracle, which is what lets the suites that
    // have no oracle of their own rely on it.
    assert!(acl::has_ace(&root, &sid).expect("read the DACL"));

    // `revoke` is still the primitive that takes one back, and the only one: an
    // operator who wants the ACE gone runs it, not a rollback this crate no longer
    // performs.
    acl::revoke(&root, &sid).expect("take the grant back by hand");
    assert!(!acl::has_ace(&root, &sid).expect("read the DACL"));
}

#[test]
fn a_grant_leaves_a_foreign_ace_alone_and_a_revoke_removes_exactly_it() {
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

    let written = ace_lines(&icacls(&root, &[]), &root);
    assert_eq!(
        written.len(),
        before.len() + 1,
        "the grant added exactly one ACE and left every other ACE as it found it: {written:?}"
    );

    acl::revoke(&root, &sid).expect("take the grant back by hand");

    let after = ace_lines(&icacls(&root, &[]), &root);
    assert!(
        !after.iter().any(|line| line.contains(&sid_text)),
        "the package ACE is gone: {after:?}"
    );
    // The half that does not depend on how a SID renders, which is the half the
    // first draft of this test got wrong: `icacls` prints `S-1-5-32-545` as
    // `BUILTIN\Users` (and the name is localised), so the property — removing this
    // SID's entry leaves every other ACE as it found it — is asserted as an
    // equality of the ACE lines rather than as a search for a string the tool never
    // prints.
    assert_eq!(
        before, after,
        "the revoke removed exactly one ACE and left every other ACE as it found it"
    );
}

#[test]
fn revoking_an_absent_ace_succeeds() {
    // Idempotence is what makes a crashed run's residue removable twice, and what
    // lets a test clean up a directory whose grant it cannot be sure landed.
    // Measured on this host 2026-09-21: `/remove:g` for a SID with no entry
    // anywhere under the path exits 0 — "Successfully processed 1 files; Failed
    // processing 0 files" — so this is the tool's behaviour rather than a kindness
    // this crate adds.
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = sid_for(dir.path());
    acl::revoke(dir.path(), &sid).expect("first revoke");
    acl::revoke(dir.path(), &sid).expect("second revoke");
}

#[test]
fn dropping_the_grants_leaves_them_standing() {
    // **The inversion of what this test asserted before the grants became
    // standing** (design decision 22's cost follow-up): `Grants` owns the profile's
    // lifetime, so dropping it deletes the *profile* and nothing else — a guard
    // that quietly took an ACE back would be the per-launch descriptor write the
    // whole change exists to remove, and it would also mean a crash and a clean
    // exit left different trees.
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
    // directory, which is the thing the contract is about.
    assert!(
        icacls(dir.path(), &[]).contains(sid.as_str()),
        "the grant is standing: dropping the guard removed nothing"
    );
}

#[test]
fn the_predicate_refuses_a_path_it_cannot_read() {
    // `has_ace` is the oracle the rest of this phase's suites consult, and an
    // oracle that answered "no entry for this SID" for a path that could not be
    // read would make every "the ACE is gone" assertion pass for the wrong reason.
    // Measured on this host 2026-09-21: a path that does not exist fails the read
    // with `ERROR_FILE_NOT_FOUND`. The read asks the DACL through
    // `GetNamedSecurityInfoW` and no longer through `icacls`, so the step the
    // failure names is the read's own.
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("never-created");
    let sid = sid_for(dir.path());

    let err = acl::has_ace(&missing, &sid).expect_err("a path the read cannot see is an error");
    let text = err.to_string();
    assert!(text.contains("read the DACL"), "{text}");
    assert!(
        text.contains("never-created"),
        "the failure names the path it was about: {text}"
    );
}

/// **The object the grant checks is the object it writes**, which is the defect
/// the handle route closed: a junction is planted at the path the whitelist
/// resolved — the window Fix 1 is about — and the grant refuses it rather than
/// following it to wherever it points.
///
/// **What the refusal protects is measured rather than asserted** (this host,
/// 2026-09-27): `icacls /grant:r` through a junction **above** the last component
/// resolved it and wrote the ACE on the target, outside the exposed tree, and the
/// target's pre-existing children inherited it. The second half of this test is
/// that no ACE reached the target: the crate's own predicate *and* the tool's own
/// rendering both say so, because an assertion that rested on `has_ace` alone
/// would also pass for a DACL nothing had looked at.
#[test]
fn a_grant_refuses_a_junction_instead_of_following_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let real = dir.path().join("real");
    fs::create_dir_all(&real).expect("create the junction's target");
    let link = dir.path().join("swapped");
    testing::junction(&link, &real);
    let sid = Arc::new(sid_for(dir.path()));
    let sid_text = sid.as_str().to_string();

    let mut grants = acl::Grants::new(Arc::clone(&sid));
    let err = grants
        .apply(&link, Target::Directory, AccessMode::ReadOnly)
        .expect_err("a junction is refused, not followed");
    assert!(err.to_string().contains("is a reparse point"), "{err}");

    assert!(
        !acl::has_ace(&real, &sid).expect("read the DACL"),
        "the ACE landed on the junction's target: {}",
        icacls(&real, &[])
    );
    assert!(
        !icacls(&real, &[]).contains(&sid_text),
        "the target carries no package ACE: {}",
        icacls(&real, &[])
    );

    // **The junction's own object is untouched too**, which is the half the old
    // wildcard-style route could take: a grant that follows the link writes there
    // instead of nowhere, and the confined child cannot use an ACE on a link
    // object. A DACL read of the link's own object is what sees it.
    let internal = acl::has_ace(&link, &sid).expect("read the link's own DACL");
    assert!(!internal, "nothing was written on the link's object either");
}

#[test]
fn a_grant_on_a_path_icacls_cannot_see_fails() {
    // The failure the launcher's startup depends on: a path that cannot be
    // resolved is refused loudly rather than skipped as "nothing to do". There is
    // no rollback list to consult any more — the standing contract has no such
    // thing — so what this pins is the error and its path, not a bookkeeping state.
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("never-created");
    let sid = Arc::new(sid_for(dir.path()));
    let mut grants = acl::Grants::new(Arc::clone(&sid));

    let err = grants
        .apply(&missing, Target::Directory, AccessMode::ReadOnly)
        .expect_err("a grant on a path icacls cannot see fails");
    assert!(err.to_string().contains("never-created"), "{err}");
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

/// **The sibling set goes on through one invocation, for the SID every container
/// already holds** (Ruling 78, decision 22, and the standing follow-up). The
/// directory's pattern is the grant the launcher actually makes, and every sibling
/// it covers is asserted through the oracle rather than through one path: a batch
/// that granted only the first match would leave a confined child that dies
/// `0xC0000135` with no diagnostic, which is the failure the counter-shape exists
/// to prevent.
///
/// The SID is [`ALL_APPLICATION_PACKAGES`] rather than this root's, which is the
/// half that makes a new root cost nothing: the program directory does not change
/// between roots, so its grant cannot either. The ACE is never taken back, so the
/// assertion is that it is still there afterwards.
#[test]
fn the_sibling_batch_grants_every_dll_for_all_application_packages() {
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

    // The precondition that makes the assertion below mean the batch ran: this
    // temp tree does not already carry the standing SID, so its presence after the
    // call is the call's doing. A host whose temp parent chain already inherits
    // ALL APPLICATION PACKAGES fails here — which is the honest reading, because
    // the grant would then have been skipped as already satisfied.
    for sibling in &siblings {
        assert!(
            !acl::mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
            "the fixture already carries the standing SID on {}: {}",
            sibling.display(),
            icacls(sibling, &[])
        );
    }

    let mut grants = acl::Grants::new(Arc::clone(&sid));
    assert!(
        grants
            .apply_siblings(&SiblingScan::scan(dir.path()), AccessMode::ReadOnly)
            .expect("one batched grant"),
        "the directory holds siblings, so the grant ran"
    );
    for sibling in &siblings {
        assert!(
            acl::mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
            "the batch granted {} for {ALL_APPLICATION_PACKAGES}: {}",
            sibling.display(),
            icacls(sibling, &[])
        );
    }

    // Standing: dropping the guard takes nothing back.
    drop(grants);
    for sibling in &siblings {
        assert!(
            acl::mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
            "the batch grant is standing on {}: {}",
            sibling.display(),
            icacls(sibling, &[])
        );
    }
}

/// A directory with no sibling is a grant of nothing: no `icacls` invocation and
/// no pattern handed to the tool. **An unmatched pattern is an error rather than a
/// no-op** (measured 2026-09-21), so this is the negative that keeps a grant on a
/// DLL-less directory from reporting success.
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
}
