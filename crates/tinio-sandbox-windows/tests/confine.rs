//! End-to-end confinement: the mechanism half of the confined-launch flow legs,
//! plus the child role the other suites spawn.
//!
//! **The flow halves moved on 2026-10-01, and each carrier is named beside the
//! leg it left** (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`):
//! the whitelist's loopback round trip, the containment of an escape and the
//! standing grants are `crates/tinio-e2e/tests/features/confinement.feature`'s
//! now ("A confined server completes a real S3 journey", "An escaping junction
//! inside the root is refused as nonexistent", and every `the run was confined`
//! scenario's `icacls` cross-check), with the standing ACE read again after the
//! launcher's death by `crates/tinio-server/tests/sandbox_e2e_windows.rs`'s
//! documented Rust leg; the read-only journey is that file's "A confined
//! read-only server refuses the write and still serves the read"; the launcher's
//! death taking its child is its "The launcher's death takes the confined
//! child"; the ACL ledger is its "The ACL ledger records the grants this run
//! made"; the capability set is its "A valid capability name is honoured" /
//! "An unknown capability name fails the run", with the deleted capability leg's
//! out-of-root refusal folded into the whitelist leg below; and the two per-`Grants` duplicate
//! legs are folded into `tinio-sandbox-windows/src/acl.rs`'s
//! `the_grant_refuses_a_path_it_cannot_open` and
//! `a_satisfied_grant_is_not_written_twice`.
//!
//! **What stays is what no product surface can ask for.** A confined child's own
//! attempt to reach a path no exposure covers is refused by the OS — the product
//! never touches an out-of-root path, so no step can induce it — and the failed
//! launch and the silent child are seams only the launcher itself presents. Each
//! is asserted by what it left behind — the child's probe file, the relay's
//! answer, the standing ACE on the operator's directory — never by the absence of
//! an error.
//!
//! **`Grants` no longer takes anything back** (design decision 22's cost
//! follow-up: one descriptor write on a directory propagates over its whole
//! subtree, measured 10–20 s for this workspace's `target\debug`), so what the
//! failed launch below asserts is the ACE that stays after it. What the guard
//! still owns is the profile's lifetime.
//!
//! **These legs fail on this host, and that is the API's answer rather than the
//! code's**: `CreateAppContainerProfile` refuses here with
//! `hr=0x80070005` (`E_ACCESSDENIED`, measured 2026-10-01), so every leg that
//! launches a confined child is red on this machine. No leg here skips — there is
//! no precondition helper to take, and a host that cannot create the profile owes
//! a red leg rather than a green one that asserted nothing (`docs/tests.md`'s
//! fail-not-skip rule).

#![cfg(target_os = "windows")]

// **The harness is the library's now.** `tests/common/` was folded into
// `tinio_sandbox_windows::testing` by the 2026-10-01 placement cleanup
// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`), because the
// confined-launch legs it serves moved inline beside their subjects and a
// `tests/` module cannot cross that boundary. The alias keeps every `common::`
// call site below reading as it did, and this binary keeps its own top-level
// `child_role` shim — the name the harness spawns here, because the lib linked
// into an integration target is compiled without `cfg(test)`
// (`tinio_sandbox_windows::testing::CHILD_ROLE`).
use std::{fs, net::TcpListener};

use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, LaunchPlan, Sandbox};
use tinio_sandbox_windows::{
    Backend, Capability, Options, acl, sid::ALL_APPLICATION_PACKAGES, testing as common,
};

/// The confined child. The body lives in the harness because the network and
/// signal suites launch the same role from their own test binaries.
///
/// **Kept, and it cannot move**: it is the test *name* the harness's payload
/// invokes in this binary (`tinio_sandbox_windows::testing::CHILD_ROLE`), so it
/// is the spawn target itself rather than an assertion — a cucumber step spawns
/// the product's binary, never this test binary.
#[test]
fn child_role() {
    common::child_role();
}

/// **The whitelist's other half, and the one no product surface can ask for**: a
/// confined child is refused a read and a write on a path **outside every
/// exposure** — the probe is the harness's `denied` sibling, which carries
/// `Users` and no package-SID ACE (the verified row
/// `tinio-sandbox-windows/src/process.rs`'s leg reproduces at the mechanism
/// layer).
///
/// **The flow halves of the leg this replaces are carried elsewhere and were
/// deleted with it** (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`):
/// the loopback round trip is `confinement.feature`'s "A confined server
/// completes a real S3 journey" (a signed S3 journey, strictly stronger than the
/// bare handshake), the containment of an out-of-root object is that file's "An
/// escaping junction inside the root is refused as nonexistent", and the standing
/// ACEs are every `the run was confined` scenario's `icacls` cross-check plus
/// `crates/tinio-server/tests/sandbox_e2e_windows.rs` (the ACE read while a run
/// serves and again after a hard kill). What stays is the fact none of those
/// reaches: the child's own attempt is refused **by the OS**, for read and for
/// write. The read half also survives in
/// `tinio-sandbox-windows/src/process.rs`'s
/// `a_rust_binary_runs_inside_the_container_and_the_whitelist_holds`; the write
/// half has this leg as its only home.
///
/// **On every volume the harness is pointed at**, because the refusal is the
/// filesystem's own access check and this is the suite's one leg that loops the
/// volumes `common::volumes` selects (the gate script's C: and E:).
///
/// The in-root lines are the controls: the child reached its probes at all
/// (`granted-read: OK` — without it, a report that never got written would make
/// the refusals below assertions about nothing) and it wrote its own root
/// (`root-write: ALLOWED` — the positive the two refusals are read against).
///
/// **The same case runs once more with a capability in the token** — the
/// assertion the deleted `a_capability_from_the_options_reaches_the_container`
/// carried: a non-default `Options::capabilities` list must not widen the
/// container past the whitelist.
#[test]
fn the_whitelist_refuses_a_path_outside_the_exposures() {
    for volume in common::volumes() {
        let (_dir, tree) = common::tree(&volume);
        let sid = common::sid_for(&tree);
        let run = common::run_confined(&tree, AccessMode::ReadWrite);
        assert_eq!(run.status, 0, "on {}: {}", volume.display(), run.report);
        assert!(
            run.report.contains("granted-read: OK"),
            "on {}: the child never reached its probes, so the refusals below would be \
             assertions about nothing: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("root-write: ALLOWED"),
            "on {}: the exposed root is writable, so the refusals below are about the \
             whitelist rather than about a child that can reach nothing: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("denied-read: refused"),
            "on {}: a path no exposure covers was readable by the confined child: {}",
            volume.display(),
            run.report
        );
        assert!(
            run.report.contains("denied-write: refused"),
            "on {}: a path no exposure covers was writable by the confined child: {}",
            volume.display(),
            run.report
        );
        // **The same refusal with a capability in the token**, on its own tree so
        // the line below is this run's and not the case above's (the report file
        // is append-only). This is the assertion
        // `a_capability_from_the_options_reaches_the_container` carried until the
        // 2026-10-01 cleanup deleted the leg: a non-default `Options::capabilities`
        // list must not widen the container past the whitelist. The list reaching
        // the container's token at all is `backend::tests`' capability-SID legs
        // (the leg's own doc named them as the only measurement that can tell a
        // read list from an emptied one), and the "still confines" half is
        // `confinement.feature`'s "A valid capability name is honoured".
        let (_cap_dir, cap_tree) = common::tree(&volume);
        let capped = common::run_confined_with(
            &cap_tree,
            AccessMode::ReadWrite,
            Options {
                capabilities: vec![Capability::InternetClient],
                ..Default::default()
            },
        );
        assert_eq!(
            capped.status,
            0,
            "on {}: {}",
            volume.display(),
            capped.report
        );
        assert!(
            capped.report.contains("denied-read: refused"),
            "on {}: a granted capability opened a path no exposure covers: {}",
            volume.display(),
            capped.report
        );
        let cap_sid = common::sid_for(&cap_tree);
        acl::revoke(&cap_tree.root, &cap_sid).expect("take the capability tree's root grant back");
        acl::revoke(&cap_tree.report, &cap_sid)
            .expect("take the capability tree's report grant back");

        // Clean up the operator's temp tree by hand: the crate does not do it,
        // and a residue in this suite's own temp tree is this suite's to take back.
        acl::revoke(&tree.root, &sid).expect("take the root's grant back");
        acl::revoke(&tree.report, &sid).expect("take the report's grant back");
    }
}

#[test]
fn a_failed_launch_leaves_the_standing_grants_and_drops_the_profile() {
    // Design §11's failed launch: the grants that were written before the failure
    // stay (there is no rollback any more), and what the failure takes with it is
    // the **profile** — the value that held it drops with the guard, and a launch
    // that never ran has no user for it.
    //
    // **Kept, and no step can induce it.** The seam is `Options::program`, and it
    // has to name a file that exists (ruling 21): `exposures()` adds the program's
    // directory to the whitelist, so a path under a directory that does not exist
    // fails `canonical_exposures` *before* a single ACE is granted — and the state
    // this test is about, a launch that fails *with the ACLs already on*, would
    // never be reached. A file that canonicalises and is not a runnable image is
    // what makes `CreateProcessW` the failing step instead. The product surface
    // cannot present that seam: the CLI's program is always the `tinio` binary,
    // which is a runnable image, so `confinement.feature` has nothing to drive.
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
    // The served root's grant is on the disk, and staying there is the contract.
    assert!(
        acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the grant written before the failure is standing"
    );
    // **And the program's own directory carries the standing SID rather than this
    // root's**, which is the half that makes a new root cost nothing: the entry is
    // asserted through the tool's own text because it names a well-known SID that
    // `has_ace` (a `PackageSid` predicate) cannot ask about.
    assert!(
        acl::mentions_sid(&common::icacls(&program_dir, &[]), ALL_APPLICATION_PACKAGES),
        "the program directory's grant is the standing one:\n{}",
        common::icacls(&program_dir, &[])
    );
    acl::revoke(&tree.root, &sid).expect("take the root's grant back");
}

#[test]
fn a_child_that_reports_no_frame_leaves_the_relays_answer_empty() {
    // Decision 19's other half, and the half only the launcher can assert: the
    // parent is the sole witness to a child that died without reporting, and the
    // read that makes its answer final is the wait's join with the relay. The
    // harness's role writes its report and answers the handshake but emits no
    // frame at all, so what this asserts is that the child really ran (its report
    // is on the disk), that the relay recorded nothing, and that the grant is
    // standing after the wait — the wait no longer writes anything at all.
    //
    // **Kept, and no step can induce it.** It needs a child that answers the
    // handshake and then emits no frame at all, which no product run presents:
    // the CLI's child either frames readiness or leaves an `error` frame (P3's
    // scenario), and a step layer cannot replace the child of a spawned binary.
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
        acl::has_ace(&tree.root, &sid).expect("read the DACL"),
        "the wait wrote nothing on the way out: the grant is standing"
    );
    acl::revoke(&tree.root, &sid).expect("take the grant back");
}
