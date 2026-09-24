//! Confined `tinio server --sandbox` on Windows: spawn the CLI, talk to it, and
//! check what only a separate process can show — launcher death takes the
//! child, a hard kill leaves residue, a clean run takes that residue over,
//! and an escaping junction is refused inside the container.
//!
//! Preconditions this host cannot meet fail unless `TINIO_SANDBOX_ALLOW_SKIP`
//! is set, and that decision happens before the first spawn. A request that
//! is not answered, an ACE that is not rolled back, or a profile that is not
//! removed fails regardless.

#![cfg(all(windows, feature = "sandbox"))]

mod common;

#[path = "support/windows_e2e_harness.rs"]
mod harness;

use std::{
    fs,
    net::TcpStream,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use common::signed;
use harness::{
    Run, ace_count, answer, exit_timeout, grant_event, host_ready, icacls, junction, wait_for_exit,
    without_ansi,
};
use tinio_sandbox::{
    AccessMode,
    windows::acl::{self, Target},
};

#[test]
fn a_confined_server_serves_and_the_launchers_death_takes_it_with_the_ace_left_behind() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    if !host_ready() {
        return;
    }
    let mut run = Run::spawn(&root, 0);

    answer(run.address);

    let sid = run.package_sid();
    assert_eq!(
        ace_count(&icacls(&root), &sid),
        1,
        "one ACE while the server runs"
    );

    // Shared §7's `info!` half, read from the launcher's own stderr as it stands:
    // this is what an operator has after a run, and a launch that left no such
    // line is a launch nobody can reconstruct. **The grant event is the assertion
    // and not a banner**: the launcher reports the package SID it was about to
    // grant and the exposure set it was about to grant it on as the fields of one
    // `info!` event, so what this leg requires is that line — the event name, the
    // SID, the exposure count — rather than a prefix any producer could print.
    // `CreateProcessW` is the backend's own "single most useful line in the file"
    // — the program and the pid, which is also the group a forwarded stop
    // addresses.
    let log = run.log_text();
    let grant = grant_event(&log).unwrap_or_else(|| {
        panic!(
            "the launcher did not report the ACL grant it was about to make: no line of its log \
             carried `step=\"grant the ACLs\"`. The launcher's log:\n{log}"
        )
    });
    assert!(
        grant.contains(&sid),
        "the grant event does not name the package SID the profile was created under, so the two \
         halves of the run cannot be put together: {grant}\n{log}"
    );
    assert!(
        grant.contains("exposures="),
        "the grant event does not say how many exposures it was about to grant, which is the \
         other field an operator reads: {grant}\n{log}"
    );
    assert!(without_ansi(&log).contains("CreateProcessW"), "{log}");

    // Kill the launcher, not the child: KILL_ON_JOB_CLOSE is the only thing that
    // can end the confined server now (decision 20, first half).
    run.launcher.kill().expect("kill the launcher");
    // The bound here and the connect bound below are the harness's one knob
    // (`harness::exit_timeout`), so a host whose ACL writes are slow raises both
    // at once instead of moving one and leaving the leg on the other.
    let deadline = Instant::now() + exit_timeout();
    loop {
        match TcpStream::connect_timeout(&run.address, exit_timeout()) {
            Err(_) => break,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
            Ok(_) => panic!("the confined server outlived its launcher"),
        }
    }
    wait_for_exit(&mut run.launcher, &run.log, "the killed launcher");

    // **The residue is asserted as a residue, not as an absence.** `kill` is
    // `TerminateProcess`, so the launcher's rollback never ran and the ACE is
    // still on the root — which is what the design's risk table says happens, and
    // a test claiming otherwise would be asserting something the mechanism cannot
    // deliver. What decision 21 promises is that the residue is *identifiable and
    // reusable*, and that is the next test's subject.
    //
    // **The SID compared is the one the launcher reported, never the `S-1-15-2-`
    // prefix**: `S-1-15-2-1` is ALL APPLICATION PACKAGES, which every directory an
    // AppContainer process has touched already carries, so a substring test would
    // find that entry and call it this run's residue.
    let residue = icacls(&root);
    assert_eq!(
        ace_count(&residue, &sid),
        1,
        "the hard kill left its ACE:\n{residue}"
    );
}

#[test]
fn a_restarted_launcher_takes_the_residue_over_and_a_clean_exit_keeps_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");

    if !host_ready() {
        return;
    }
    let mut first = Run::spawn(&root, 0);
    let sid = first.package_sid();
    first.launcher.kill().expect("kill the first launcher");
    wait_for_exit(&mut first.launcher, &first.log, "the killed first launcher");
    assert_eq!(
        ace_count(&icacls(&root), &sid),
        1,
        "the first run's residue is on the root"
    );

    // The same root derives the same package name, so the same SID — and
    // `/grant:r` replaces this SID's entry rather than adding a second one. That
    // pair *is* decision 21's claim, and it is the only form of it a test can
    // assert: the residue is reusable, not accumulated.
    let mut second = Run::spawn(&root, 0);
    assert_eq!(
        second.package_sid(),
        sid,
        "the same root derives the same SID"
    );
    let taken = icacls(&root);
    assert_eq!(
        ace_count(&taken, &sid),
        1,
        "the second run took the entry over rather than adding one:\n{taken}"
    );

    // **End the run with the launcher alive**, which is the only way its teardown
    // runs at all: the console control event goes to the launcher's group, the
    // launcher's relay forwards it to the child (design §3 step 9), the child's
    // stop hook shuts it down, and the launcher — still running — deletes the
    // profile. **The ACEs stay**: every grant this crate makes is standing, so a
    // clean exit keeps the residue the hard kill left. Killing the *launcher*
    // would skip all of that, which is why the test above asserts a residue and
    // this one asserts it survives the clean exit too.
    //
    // **And the stop is not asserted on its own return value.** A console-wide
    // event never reaches a `CREATE_NEW_PROCESS_GROUP` child, and an accepted call
    // is not a delivered one (Task 10's measurements) — so what this leg reads is
    // the child's own effect: the launcher exits, with its child's status, and the
    // standing ACE is still on the root.
    //
    // **A refusal here is fatal, and the host's half of the question was answered
    // before this leg spawned anything.** `send_stop` has two causes for an
    // `Err` — no console, or a group that does not exist — and only the first is a
    // property of the machine; [`host_ready`] probed it. This launcher announced
    // readiness from *this* run a moment ago, so its group exists and is
    // addressable: a refusal now means it died between the takeover and the stop,
    // which this leg's own doc block says must not happen. An earlier revision let
    // the operator's opt-out swallow exactly that, turning a launcher that died
    // mid-run into a pass — the same defect class as the readiness frame.
    let status = second.stop("the stopped launcher");
    assert!(
        status.code().is_some(),
        "the launcher reported its child's status"
    );
    let after = icacls(&root);
    assert_eq!(
        ace_count(&after, &sid),
        1,
        "the clean exit took the standing ACE back:\n{after}"
    );

    // **And the profile went with it — decision 21's other artifact, which the
    // ACE count above cannot see.** A launcher that kept the ACE and left the
    // profile behind is green on every line before this one, and that is not
    // a hypothetical: the phase's diagnosis found eight profiles leaked by runs
    // that exited cleanly, because a `process::exit` had landed above the `Drop`
    // that deletes them, and the code's own doc says any later `process::exit` in
    // that position reintroduces it. The observable is a *third* run on the same
    // root: `taken_over` is `false` only if the second run deleted the profile,
    // and `true` is what a survivor looks like from outside. Read from the log
    // rather than re-derived here, because a `PackageSid` built in *this* process
    // would create the very profile the assertion is about.
    //
    // **The third run is stopped rather than killed**, so it takes back what it
    // made: this leg's residue is the first run's hard kill, which is its subject,
    // and a second orphaned profile per suite run would be the suite's own.
    let mut third = Run::spawn(&root, 0);
    assert!(
        !third.took_the_profile_over(),
        "the clean exit left the profile behind: this run found one to take over"
    );
    // Its status is not this leg's subject — the third run exists to read one
    // field — so the wait is what is asserted: a third launcher that outlives the
    // stop is a relay failure, and [`Run::stop`] reports it with the log.
    let _ = third.stop("the stopped third launcher");
}

/// **The containment proof has to keep resolving reparse points *inside* the
/// container, and this is the only place in the suite that asks.** Every other
/// leg serves a tree with no link in it, so the proofs they exercise would pass
/// whether they resolved anything or not.
///
/// The shape is the one the vendored `soft-canonicalize` patch was measured to
/// accept: a junction **inside** the served root that lands **outside** it. The
/// proof is `strict-path`'s `strict_join`, which canonicalizes the candidate and
/// refuses it when the answer leaves the boundary — so an answer that never
/// resolved the junction passes the proof, and the escaping directory is a
/// bucket like any other. Outside the container the same tree is refused,
/// because the platform canonicalizer is not refused there; inside, that
/// refusal is the property the patch has to preserve.
///
/// **The request is a `GET` of an object under the escaping bucket, and the
/// discriminator is the `NoSuchBucket` *code* — not the 404 status.**
/// `ensure_bucket` asks `FsStorage::bucket_dir` first, which is the
/// containment proof and nothing else. When the proof refuses, that error would
/// surface on its own; what actually turns it into `NoSuchBucket` is
/// `ensure_bucket`'s **reparse fallback** (`objects.rs`), which reads the
/// bucket path with `symlink_metadata`, sees the reparse point, and rewrites
/// *any* `bucket_dir` error into `NoSuchBucket` when `follow_symlinks` is
/// off — for this bucket shape whatever the proof decided. The 404 is therefore
/// `s3s`'s mapping of that code rather than the proof's own status, and a bare
/// status assertion here would not discriminate: without the fallback the same
/// leg would assert the other way round. When the proof does not refuse, the
/// bucket is there, the read proceeds far enough to meet the **object-level
/// lstat policy** — `check_symlinks`, which walks every existing component up
/// to the root and refuses the link with `AccessDenied` — and 403 `AccessDenied`
/// is the answer instead, because that path reaches the policy before any bucket
/// resolution. So the two worlds differ in the code the data plane emits:
/// `NoSuchBucket`, whose fallback is what makes a reparse-point bucket
/// look absent, against `AccessDenied`. Measured both ways on this host; the
/// leg asserts the first.
///
/// **A bucket listing is the wrong probe, and that is measured too.** `GET
/// /<bucket>` answers `InternalError` in *both* worlds: `listing.rs`'s
/// `walk_files_streaming` maps the bucket with the lexical join and, for a
/// reparse-point bucket under `follow_symlinks: false`, calls the **platform**
/// canonicalizer — which the container refuses for every path — so a junction
/// bucket is an I/O error there whatever the proof decided. Nothing about this
/// leg may be read as covering that site.
///
/// **The junction's target is granted to this run's package SID, and the grant
/// is the premise rather than the subject.** The resolution adopts a link's
/// target only when the target (or its parent) exists, and `exists()` swallows
/// the refusal — so against an ungranted target the proof answers lexically in
/// *both* worlds and the leg would assert nothing. The grant models the case the
/// proof is for: a directory outside the root that the container can reach.
/// (It is read off the launcher's own grant event and applied between the launch
/// and the request, because the confined child resolves the path per request; the
/// SID is never re-derived here, since a `PackageSid` built in this process
/// would create the profile the launcher is using.)
///
/// **What it loses if the patch's prefix arms are reverted**: the proof accepts
/// the bucket, the fallback never runs, and the object-level policy answers
/// `AccessDenied` instead — so the code assertion fails where this leg requires
/// `NoSuchBucket`. The control request below reads a plain directory and is
/// unaffected, so the leg cannot pass by refusing everything, and cannot pass on
/// a lexical proof either.
#[tokio::test]
async fn a_confined_server_refuses_an_escaping_bucket_junction_as_nonexistent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    let outside = dir.path().join("outside");
    if !host_ready() {
        return;
    }
    // The launcher creates the served root itself on a normal run; the junction
    // has to be inside it before the confined child resolves anything, so the
    // tree is planted here and `create_dir_all` on an existing root is a no-op.
    fs::create_dir_all(&root).expect("the served root");
    // The control bucket: a plain directory with one object in it, which this
    // leg reads to show the confined server does serve this tree.
    fs::create_dir_all(root.join("plain")).expect("the control bucket");
    fs::write(root.join("plain").join("plain.txt"), b"plain").expect("seed the control object");
    fs::create_dir_all(&outside).expect("the out-of-root target");
    fs::write(outside.join("outside.txt"), b"outside").expect("seed the out-of-root target");
    junction(&root.join("leak"), &outside);

    let mut run = Run::spawn(&root, 0);

    // The spec is `acl::grant_spec`'s and not a mask spelled here: it is the
    // launcher's own call with one string taken from the grant event instead of
    // from a `PackageSid` value, and the leading `*` that stops `icacls` resolving
    // it as an account name is the crate's rule rather than this test's.
    let sid = run.package_sid();
    let spec = acl::grant_spec(&sid, Target::Directory, AccessMode::ReadOnly);
    let granted = Command::new(acl::icacls_binary())
        .arg(&outside)
        .args(["/grant:r", &spec, "/q"])
        .output()
        .expect("run icacls");
    assert!(
        granted.status.success(),
        "grant the run's package SID read on {}: {}",
        outside.display(),
        String::from_utf8_lossy(&granted.stderr)
    );

    // **The control first.** A signed request is the only kind that reaches the
    // storage — an unsigned one is refused `Signature is required` — and this
    // one proves the confined server is serving *this* tree, so a refusal below
    // cannot be a server that failed to start.
    let control = signed(run.address, "GET", "/plain/plain.txt", b"").await;
    assert_eq!(
        control.status,
        200,
        "the confined server must serve a plain object of this tree: {} {}",
        control.status,
        control.text()
    );

    let answer = signed(run.address, "GET", "/leak/outside.txt", b"").await;
    // **The code is the assertion and the status is its consequence.** The
    // discriminator is `NoSuchBucket`: it is what `ensure_bucket`'s reparse
    // fallback emits once the proof has refused the bucket, and the failing world
    // emits `AccessDenied` from the object-level policy instead. The 404 below is
    // the wire form `s3s` gives that code, asserted so the leg still pins the
    // HTTP shape — this leg does not own the number, and the code is what tells
    // the two worlds apart.
    assert_eq!(
        answer.code(),
        "NoSuchBucket",
        "a bucket whose directory is a junction out of the served root must be invisible to the \
         data plane — the containment proof resolved the link, refused it, and `ensure_bucket`'s \
         reparse fallback answered `NoSuchBucket` for the refused bucket. `{}` (status {}) is a \
         different world: the object-level symlink policy (`check_symlinks`) catching a bucket \
         the proof accepted, which is what a reverted prefix arm produces; 500 is the read \
         reaching a directory the container cannot open:\n{}",
        answer.code(),
        answer.status,
        answer.text()
    );
    assert_eq!(
        answer.status,
        404,
        "`NoSuchBucket` is mapped to 404 on the wire; a different status for that code is a \
         protocol change this leg should notice:\n{}",
        answer.text()
    );

    // **Ended with a stop rather than a kill**, so the launcher rolls back what
    // it granted — the executable's directory among the exposures, and the
    // served root. A hard kill would leave both as this suite's residue on every
    // run of this leg.
    // This leg's subject is the answer above; the wait is asserted because a
    // launcher that outlives its stop is a relay failure [`Run::stop`] reports.
    // Its status is the child's and is not read here.
    let _ = run.stop("the stopped launcher");
}
