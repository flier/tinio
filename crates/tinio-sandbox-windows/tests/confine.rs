//! End-to-end confinement: the whitelist, the loopback round trip through the
//! inherited listener, and the rollback on every exit path — on every volume the
//! harness is pointed at.
//!
//! **The failure paths are as much this suite's subject as the clean one.** A
//! grant that fails, a launch that fails with the ACLs already on, a supervision
//! that is dropped instead of waited on, and a child that never reports: each is
//! asserted by what it left behind — an ACE on the operator's directory, the
//! child's probe file, the relay's answer — never by the absence of an error.

#![cfg(windows)]
// `Grants::new` takes an `Arc<PackageSid>` over a type that owns a raw `PSID` and
// is deliberately neither `Send` nor `Sync`, which is what clippy's
// `arc_with_non_send_sync` is about; this one is shared ownership inside one test
// rather than an `Arc` meant to cross a thread boundary, which is what the code
// under test is given. The same allow, for the same reason, is in `tests/acl.rs`.
#![allow(clippy::arc_with_non_send_sync)]

mod common;

use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, LaunchPlan, Sandbox};
use tinio_sandbox_windows::{
    Backend, Capability, Options,
    acl::{self, Target},
    sid::PackageSid,
};

/// The confined child. The body lives in the harness because the network and
/// signal suites launch the same role from their own test binaries.
#[test]
fn child_role() {
    common::child_role();
}

#[test]
fn the_whitelist_holds_the_loopback_round_trip_and_the_rollback() {
    for volume in common::volumes() {
        let (_dir, tree) = common::tree(&volume);
        let sid = common::sid_for(&tree);
        let run = common::run_confined(&tree, AccessMode::ReadWrite);
        assert_eq!(run.status, 0, "on {}: {}", volume.display(), run.report);
        // The pre-existing file this reads was seeded before the grant: the
        // functional half of design §6's inheritance answer (Task 4).
        assert!(
            run.report.contains("granted-read: OK"),
            "on {}: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("denied-read: refused"),
            "on {}: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("denied-write: refused"),
            "on {}: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("root-write: ALLOWED"),
            "on {}: {}",
            volume.display(),
            run.report
        );
        // Decision 15: the ACEs are gone once the launcher is done, on the clean
        // path as much as on the failure paths. **`has_ace` refusing a non-zero
        // `icacls` exit is what makes these assertions mean anything** (Task 4):
        // as an oracle it would otherwise have reported "no entry" for a path it
        // could not read at all, and every rollback assertion in this file would
        // pass for that reason rather than this one.
        assert!(
            !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
            "on {}",
            volume.display()
        );
        assert!(
            !acl::has_ace(&tree.report, &sid).expect("read the DACL"),
            "on {}",
            volume.display()
        );
    }
}

#[test]
fn a_read_only_root_refuses_writes_and_still_reads() {
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let run = common::run_confined(&tree, AccessMode::ReadOnly);
    assert_eq!(run.status, 0, "{}", run.report);
    assert!(run.report.contains("granted-read: OK"), "{}", run.report);
    assert!(run.report.contains("root-write: refused"), "{}", run.report);
    assert!(
        !tree.root.join("written.txt").exists(),
        "the read-only root refused the write in the report but not on the disk"
    );
    assert!(!acl::has_ace(&tree.root, &sid).expect("read the DACL"));
}

#[test]
fn a_failed_launch_still_rolls_the_aces_back() {
    // Design §11: the rollback runs after a failed launch too, not only on the
    // clean path.
    //
    // **The seam is `Options::program`, and it has to name a file that exists**
    // (ruling 21): `exposures()` adds the program's directory to the whitelist, so
    // a path under a directory that does not exist fails `canonical_exposures`
    // *before* a single ACE is granted — and the rollback this test exists for, a
    // launch that fails *with the ACLs already on*, would never run. A file that
    // canonicalises and is not a runnable image is what makes `CreateProcessW` the
    // failing step instead.
    let volume = common::one_volume();
    let (dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);

    let program_dir = dir.path().join("not-an-image");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    let image = program_dir.join("serve.txt");
    fs::write(&image, b"this is not a portable executable").expect("write the file");

    let sandbox = Sandbox::new().expose(&tree.root, AccessMode::ReadWrite);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind the listener");
    let plan = LaunchPlan::for_launch(String::new()).with_listener(listener);
    let mut backend = Backend::with_options(Options {
        program: Some(image.clone()),
        ..Default::default()
    });
    let err = BackendTrait::confine(&mut backend, &sandbox, &plan)
        .expect_err("a file that is not a runnable image must fail the launch");
    assert!(
        err.to_string().contains("CreateProcessW"),
        "the failure names the step that failed: {err}"
    );
    // Both grants the exposure set resolved are off the disk: the served root,
    // and the program's own directory the launcher added for the child's loader.
    assert!(
        !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the grant was rolled back after the failed launch"
    );
    assert!(
        !acl::has_ace(&program_dir, &sid).expect("read the DACL"),
        "the program's own directory was rolled back too"
    );
}

#[test]
fn a_dropped_supervision_also_rolls_back() {
    // The guard's path: the caller never waits, and the ACEs still go.
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let (confinement, address) =
        common::launch_confined(&tree, AccessMode::ReadWrite, Options::default(), None);
    assert!(
        acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the grant is on while it runs"
    );
    drop(confinement);
    assert!(
        !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "dropping the supervision rolled the grant back"
    );
    // **And the child went with it, which is the other half of what a caller
    // walks away from.** The wait closure owns the job, so the drop closes it and
    // kill-on-close terminates whatever it still holds (decision 20). The
    // observable is the socket: the child inherited the listener and is now its
    // only owner — the parent's copy went when the plan did — so a connection that
    // stops being accepted is the fact that the child is gone. Bounded, because
    // the termination is not instantaneous and a socket that is still there
    // forever is the failure this assertion is about.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
            Err(_) => break,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            Ok(_) => panic!(
                "the confined child outlived the dropped supervision: {address} still accepts \
                 connections, so the job did not take it"
            ),
        }
    }
}

#[test]
fn a_child_that_reports_no_frame_leaves_the_relays_answer_empty() {
    // Decision 19's other half, and the half only the launcher can assert: the
    // parent is the sole witness to a child that died without reporting, and the
    // read that makes its answer final is the wait's join with the relay. The
    // harness's role writes its report and answers the handshake but emits no
    // frame at all, so what this asserts is that the child really ran (its report
    // is on the disk), that the relay recorded nothing, and that the grants went
    // back on the clean-wait path.
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let run = common::run_confined_with(&tree, AccessMode::ReadWrite, Options::default());

    assert_eq!(run.status, 0, "{}", run.report);
    assert!(
        run.report.contains("granted-read: OK"),
        "the child never reached its probes, so this test would be asserting on nothing: {}",
        run.report
    );
    assert!(
        !run.terminal,
        "a child that emitted no frame left a terminal event behind: {:?}",
        run.terminal
    );
    assert!(
        !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the wait rolled the grants back"
    );
}

#[test]
fn a_capability_from_the_options_reaches_the_container() {
    // The composition builds the container's capability list from
    // `Options::capabilities` through `CapabilitySids::resolve`, and this is the
    // observable a test has of it: a non-default list still confines, and the
    // child it launched really ran. What it cannot distinguish — and nothing here
    // can, short of a network-policy probe that Task 12 owns — is a list that was
    // read from one that was silently replaced by an empty one; the reading is
    // pinned by the one line that builds `Container` and by `backend`'s unit
    // tests on the default.
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let options = Options {
        capabilities: vec![Capability::InternetClient],
        ..Default::default()
    };
    let run = common::run_confined_with(&tree, AccessMode::ReadWrite, options);

    assert_eq!(run.status, 0, "{}", run.report);
    assert!(
        run.report.contains("denied-read: refused"),
        "the whitelist holds with a capability as well: {}",
        run.report
    );
    assert!(
        !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the capability list does not change the rollback"
    );
}

#[test]
fn a_grant_that_fails_rolls_back_what_the_earlier_ones_wrote() {
    // **The one failure path a test cannot drive through `confine`, and the
    // reason.** `confine` canonicalises every exposure before it grants anything,
    // so the only grant failures that reach its loop are `icacls`'s own refusals
    // — and `canonical_exposures` accepts anything that resolves, including the
    // shapes a test could hand it (a file, a deep path, an unusual spelling), all
    // of which `icacls` grants happily. What is left is the guard itself: the
    // object `confine` holds, and the one its failure arm and its wait closure
    // both act through. A path that is gone by the time the tool runs is a
    // refusal with the earlier grants already on the disk, which is exactly the
    // state the guard exists for.
    let (_dir, tree) = common::tree(&common::one_volume());
    let sid = Arc::new(common::sid_for(&tree));
    let mut grants = acl::Grants::new(Arc::clone(&sid));

    grants
        .apply(&tree.root, Target::Directory, AccessMode::ReadWrite)
        .expect("the first grant lands");
    assert!(
        acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the first grant is on the disk before the second is attempted"
    );

    let vanished = tree.report.join("vanished");
    let err = grants
        .apply(&vanished, Target::Directory, AccessMode::ReadWrite)
        .expect_err("a grant on a path icacls cannot see fails");
    assert!(err.to_string().contains("icacls"), "{err}");

    // Dropping the set is the path a failed grant takes inside `confine`: the
    // failure leaves the function through `?`, the guard drops, and what it takes
    // back is what it already wrote.
    drop(grants);
    assert!(
        !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the grants that landed before the failure were taken back"
    );
}

#[test]
fn a_second_grant_replaces_the_ace_instead_of_adding_one() {
    // **Decision 21's takeover property, and the only place it is pinned.** A run
    // after a hard kill derives the same package name, so it reuses the SID and
    // the ACEs a crashed run left behind — and `/grant:r` is what makes that a
    // takeover rather than a pile of duplicates. Every other grant in this phase's
    // suites lands on a path with no prior same-SID ACE, so nothing else
    // distinguishes `/grant:r` from `/grant`; the observable here is `icacls`'s
    // own rendering, one line per ACE.
    let (_dir, tree) = common::tree(&common::one_volume());
    let sid = Arc::new(common::sid_for(&tree));
    let mut grants = acl::Grants::new(Arc::clone(&sid));

    grants
        .apply(&tree.root, Target::Directory, AccessMode::ReadWrite)
        .expect("the first grant lands");
    let after_first = ace_count(&tree.root, &sid);
    assert_eq!(after_first, 1, "one grant, one ACE");

    grants
        .apply(&tree.root, Target::Directory, AccessMode::ReadWrite)
        .expect("a second grant on the same path succeeds");
    assert_eq!(
        ace_count(&tree.root, &sid),
        after_first,
        "a second grant on one path added a second ACE, so a run that took over a crashed run's \
         residue would leave two"
    );

    grants.rollback().expect("roll back");
    assert_eq!(
        ace_count(&tree.root, &sid),
        0,
        "the rollback took the one ACE back"
    );
}

/// How many ACEs `icacls` renders for `path` with `sid` in them.
///
/// The oracle is the tool's own rendering, which is where the design's evidence
/// table came from, and it is the harness's [`common::icacls`]: its exit status is
/// checked, so a count read from a refused invocation cannot be `0` for a path the
/// tool could not read at all — which would make the "did not grow" assertion
/// above pass for that reason rather than this one.
fn ace_count(path: &Path, sid: &PackageSid) -> usize {
    common::icacls(path, &[])
        .lines()
        .filter(|line| line.contains(sid.as_str()))
        .count()
}
