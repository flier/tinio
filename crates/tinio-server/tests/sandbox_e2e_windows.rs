//! Confined `tinio server --sandbox` on Windows: spawn the CLI, talk to it, and
//! check what only a separate process can show — a hard kill leaves residue, a
//! clean run takes that residue over, and a child that dies before reporting
//! readiness becomes the parent's own `error` frame. **Two facts the file used to
//! hold are `crates/tinio-e2e/tests/features/confinement.feature`'s now** (the
//! plan's §5 migration): the launcher's death taking the child is P4, and an
//! escaping bucket junction being refused as nonexistent inside the container is
//! P5 — what stays here is the ACE half, which a step layer has no package-SID
//! oracle for except the launcher's own log, and the parent's frame, whose
//! induction is Windows' own.
//!
//! Preconditions this host cannot meet fail unless `TINIO_SANDBOX_ALLOW_SKIP`
//! is set, and that decision happens before the first spawn. A request that
//! is not answered, a profile that is not removed when the launcher exits
//! cleanly, or an ACE that is silently taken back fails regardless — every
//! grant this crate makes is standing (design decision 15), so a residue is
//! the expected state and its absence is the surprise.

#![cfg(all(windows, feature = "sandbox"))]

mod common;

#[path = "support/windows_e2e_harness.rs"]
mod harness;

use std::{fs, os::windows::process::ExitStatusExt as _, process::ExitStatus};

use harness::{
    Exit, Run, ace_count, answer, grant_event, host_ready, icacls, junction, seal_to_current_user,
    step_event, wait_for_exit, without_ansi,
};
use tinio_sandbox::Event;

/// **The ACE this run holds, while it runs and after the launcher's death — the half no step can
/// read.**
///
/// The serving half and the launcher's death *taking the child* are `confinement.feature`'s P4 now
/// (the plan's §5 split): this leg's subject is the standing ACE on the served root (design
/// decision 15's residue), read while the run serves and again after a hard kill leaves it — which
/// is why the kill below is its induction rather than its subject, and why the request it makes
/// first is a control rather than a claim. The SID the ACE is compared against comes from the
/// launcher's own record, because a `PackageSid` built in *this* process would create — and on
/// `Drop` delete — the very profile the launcher is using.
#[test]
fn a_confined_server_holds_one_ace_while_it_runs_and_after_the_launcher_is_killed() {
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
    // can end the confined server now (decision 20, first half) — and the wait is
    // what makes the read below a residue rather than a race: the ACE is read from
    // the root after the process whose job held the server is gone.
    run.launcher.kill().expect("kill the launcher");
    wait_for_exit(&mut run.launcher, &run.log, "the killed launcher");

    // **The residue is asserted as a residue, not as an absence.** `kill` is
    // `TerminateProcess`, so the launcher's teardown never ran and the ACE is
    // still on the root — and it would be there had the teardown run, because
    // every grant this crate makes is standing (design decision 15), which is what
    // the design's risk table says happens and a test claiming otherwise would be
    // asserting something the mechanism cannot deliver. What decision 21 promises
    // is that the residue is *identifiable and reusable*, and that is the next
    // test's subject.
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

/// **A confined child that dies on the way up becomes the parent's own `error`
/// frame, and the status that parent answers with is the child's.**
///
/// This is design §3 step 8 and the one assertion §12 asks of the launcher
/// platforms beyond the relay
/// (`docs/superpowers/specs/2026-09-18-sandbox-design.md:552`): a child "which
/// exits without ever reporting becomes the parent's own `Event::Error`, read
/// from the terminal record **after** the wait rather than before it". The site
/// is `crates/tinio-cli/src/commands/start/run.rs`'s `launcher_exit` — the
/// sentence and the code that arm answers with — and this is the leg that reads
/// them off the process rather than off a library call: the unit legs beside
/// that function pin the *decision*, and only a spawned binary shows the write
/// and the exit status.
///
/// **The induction is a served root that is a directory junction, and the two
/// processes meet different trees.** The launcher resolves the operator's path
/// as the unconfined user: it canonicalises the junction to the directory it
/// names, builds the exposure set from **that** path (the `resolve an exposed
/// path` events carry it, and the junction's own spelling appears nowhere in the
/// grant), writes the package SID's ACE there, and launches. The child is
/// re-invoked with the launcher's own argv, so *its* first act is to
/// canonicalise the **junction**, whose entry lives outside every exposure: the
/// tree this fixture seals grants the calling user alone, a confined child's
/// access check needs an ACE for a SID of its own, and `resolve_root` therefore
/// answers the CLI's usage error (`exit 2`) — **before the dispatch**, which is
/// why nothing reaches stdout from the child at all.
///
/// **What each half of the fixture is load-bearing for.** The junction is what
/// puts the child's path outside the exposure set, where a plain directory would
/// have been granted; the seal is what makes the refusal a property of this leg
/// rather than of the host's scratch tree (`harness::seal_to_current_user`
/// carries the measurement and the reason). Neither is decoration: a fixture
/// whose link did not take fails in `junction`'s own self-assertion, and a seal
/// that did not take fails its read-back — not this leg's assertions.
///
/// **The frame is the parent's, and there is exactly one of them.** The parent
/// and the child never both frame one failure, and this is the half where the
/// child sent nothing: the assertion is therefore the stream's **bytes** against
/// the crate's own encoder — one `error` frame, carrying the parent's sentence —
/// rather than "an error appeared somewhere", and a `listening` frame (the child
/// having reported after all) fails it rather than being ignored by it.
///
/// **The status in that sentence is the child's own.** `exit 2` is the CLI's
/// usage refusal, where the launcher's answer for a child that left no status at
/// all is `1` — `launcher_exit`'s documented fallback for a child killed by a
/// signal — so the number cannot be a non-zero the parent invented for the
/// occasion.
///
/// **Three reads say a child really ran**, each of them a different process's
/// record: the launcher logged `CreateProcessW` (it got past the resolution and
/// the grants), the child's own `tinio: …` refusal names the junction path on
/// the stderr it inherited, and the launcher logged the child's exit with the
/// code the frame renders. A launch that failed before spawning fails this leg
/// instead of passing it: the parent's own refusals carry a different sentence
/// (`run`'s serving arm frames the `Err` that `start::run` returns) and its
/// pre-dispatch ones carry no frame at all.
///
/// **Not covered here, and it is stated rather than implied.** The macOS arm of
/// the same assertion — this leg is the Windows one, and the host that would run
/// the other is not this one — and the *ordering* the design sentence names
/// ("after the wait rather than before it"), which is a property of `supervise`'s
/// borrowed child and `Child::wait`'s join rather than of anything observable on
/// this side of the pipe: a parent that read the record early would compile, run,
/// and usually answer with the same frame.
#[test]
fn a_child_that_never_reports_is_the_parents_own_error_frame() {
    let dir = tempfile::tempdir().expect("temp dir");
    let tree = dir.path().join("tree");
    let real = tree.join("real");
    let root = tree.join("root");
    if !host_ready() {
        return;
    }
    // The tree, then the link, then the seal: the standing entries are written
    // over what is already inside the tree, so the junction ends up carrying the
    // calling user's ACE alone — which is the state the child is refused in.
    fs::create_dir_all(&real).expect("the directory the junction names");
    fs::write(real.join("hello.txt"), b"hello").expect("seed an object");
    junction(&root, &real);
    seal_to_current_user(&tree);

    let mut run = Exit::spawn(&root, 0);
    let status = wait_for_exit(&mut run.launcher, &run.log, "the launcher");

    // The parent's own answer is the child's status, and the log is quoted with
    // the failure because the launcher's log is what an operator reads after it.
    assert_eq!(
        status.code(),
        Some(2),
        "the launcher forwards the child's status: a confined child that cannot resolve the root \
         it was launched with leaves the CLI's usage refusal (2), where a child that left no \
         status at all would have had the launcher answering 1. The launcher's log:\n{}",
        run.log_text()
    );

    // **One frame, and it is the parent's sentence.** Comparing the whole stream
    // against the encoder's own bytes is what makes the two halves of the design's
    // sentence checkable at once: the frame is an `error`, and it is the *parent's*
    // — a relayed frame from the child would be a different `message` here, and a
    // second frame would be extra bytes.
    let stdout = run.stdout_text();
    let sentence = format!(
        "the confined child exited ({}) without reporting readiness",
        ExitStatus::from_raw(2)
    );
    assert_eq!(
        stdout,
        Event::Error { message: sentence }.encode(),
        "stdout must carry the parent's own `error` frame and nothing else — no `listening` frame, \
         since the child never reported, and no second frame for one failure. Got:\nstdout:\n\
         {stdout}\nThe launcher's log:\n{}",
        run.log_text()
    );

    // **A child ran**, and each half of the evidence is another process's record:
    // the launcher reached the launch, the launcher saw the child leave with the
    // code the frame renders, and the child's own refusal — in the child's own
    // words, on the stderr it inherited — names the path its argv carried.
    let log = without_ansi(&run.log_text());
    assert!(
        log.contains("CreateProcessW"),
        "the launcher never reached the launch, so this leg would be asserting on a parent that \
         never had a child:\n{log}"
    );
    let exited = step_event(&log, "child exit").unwrap_or_else(|| {
        panic!(
            "the launcher logged no `step=\"child exit\"` event, so the child this leg is about \
             cannot be told from one that never existed:\n{log}"
        )
    });
    assert!(
        exited.contains("code=2"),
        "the child left the status the frame renders, and the launcher's own record of the wait \
         says so: {exited}\n{log}"
    );
    assert!(
        log.contains(&format!(
            "tinio: the storage root `{}` is not usable: ",
            root.display()
        )),
        "the child's own refusal is on the stderr it inherits, and it names the operator's \
         spelling — the junction the launcher resolved on its own side:\n{log}"
    );
    assert!(
        log.contains("— pass an existing DIR"),
        "the child's refusal is `resolve_root`'s, whose remedy is the half an operator acts \
         on:\n{log}"
    );
}
