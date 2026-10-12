//! The confined server, driven the way the interop harness drives it: spawn the
//! `tinio` server, read the endpoint the confined child reports, and check what the
//! run's own records show — the launcher's published `state` and the control channel
//! it names, the confined child's write through its profile, the relay, and the
//! mechanism the launcher resolved.
//!
//! This is the CI macOS leg's acceptance (design §12). It fails rather than
//! skips on macOS: a leg that quietly passes without confining anything is the
//! failure mode the design refuses everywhere else.
//!
//! **Two halves of what this leg used to assert are the suite's now** (the plan's
//! §5 split): that a confined run *serves* is `confinement.feature`'s G8 — a signed
//! S3 journey through the same binary, strictly stronger than the bare HTTP probe
//! this leg used to assert — and that the launcher *forwards its child's status* is
//! its P2, written for both launcher platforms. What the probe below still exists
//! for is the access log the request makes the confined child write.
//!
//! **The product entry point is the `tinio` binary, not an example.** The plan
//! this file comes from drove a `serve` example; no such example exists in this
//! tree — the serving path is the CLI's (shared §4) — so this leg spawns
//! `tinio server <root> --port 0 --sandbox` through [`common::tinio_bin`], the
//! resolution every spawning target in this crate shares. The confined child's
//! process name is therefore `tinio`, which is what
//! `crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` attributes the
//! kernel's refusals by.

#![cfg(target_os = "macos")]
#![cfg(feature = "sandbox")]
// **A stated exemption because clippy's test keys do not reach it**
// (docs/style.md §Panics, design 2026-10-09 D7): `allow-*-in-tests` covers
// `#[test]` functions and `#[cfg(test)]` modules, while this target's failure
// helper (`fail`) is a plain `fn` outside one — and it is deliberately the one
// panicking shape here: it panics only after it has swept the confined grandchild
// and reaped the launcher, because a panic is what cannot run that sweep first.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

mod common;

use std::{
    env, fs,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio, id as process_id},
    thread,
    time::{Duration, Instant},
};

use common::{stdout_lines, tinio_bin, wait_for_listening_in, wait_timeout};
use tinio_api::state::State;
use tinio_sandbox::Event;
use tinio_util::{
    job,
    testing::{frame_line, kill_and_reap, poll_exit, wait_for_path},
};

/// How often the leg looks for a file the product writes: the waits below are
/// bounded by [`wait_timeout`], so the poll is their resolution and not a
/// resource.
const POLL: Duration = Duration::from_millis(50);

#[test]
fn a_confined_run_publishes_its_state_and_reports_the_mechanism_it_chose() {
    // **The served root is deliberately short, and that is a macOS fact rather
    // than tidiness.** `$TMPDIR` is `/private/var/folders/<xx>/<hash>/T/`, so a
    // root under it is already ~52 bytes and the child's unix control socket
    // `<root>/.tinio/tinio-<16 hex>.sock` then needs ~117 — past `sun_path`'s
    // **104 bytes**, where the confined server refuses to start instead of
    // serving (macOS 15.8, 2026-09-25):
    //
    // ```text
    // management error: transport: the socket path
    // /private/var/folders/…/root/.tinio/tinio-48484d7560db59a2.sock is too long
    // for `sun_path` (104 bytes): set `[api.unix] path` to an absolute path in a
    // short directory
    // ```
    //
    // `/tmp` is the platform's short scratch area, so it is the fixture's home;
    // a `tempfile::tempdir()` with a rename would only move the problem.
    let root = PathBuf::from(format!("/tmp/tinio-e2e-{}", process_id()));
    // A pid can be reused across runs, and a stale root would carry a state file
    // and a database from an unrelated run.
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create root");

    // **`job::spawn`, the harness's one spelling** — a no-op on this platform, where the module
    // has no call to make, but the sites that spawn the process under test do not each decide
    // that for themselves, so a leg moved to a platform the guard exists on is already guarded.
    let mut command = Command::new(tinio_bin());
    command
        .args([
            "server",
            root.to_str().expect("utf-8 root"),
            "--port",
            "0",
            "--sandbox",
        ])
        // **`NO_COLOR`, because the four `step=` assertions below read this stream as data.**
        // `tracing-subscriber` styles a field *name* whenever ANSI is on — `step` arrives as
        // `\x1b[3mstep\x1b[0m\x1b[2m=\x1b[0m"profile"`, measured on macOS 15.8 while the confined
        // server was serving requests — so a run with colour logs the same events yet matches none
        // of the four. The subscriber honours the standard variable and leaves a terminal's colour
        // alone (`crate::log::build_subscriber`), which is exactly the split this leg needs: the
        // assertion sets it, an operator does not have to.
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = job::spawn(&mut command).expect("spawn tinio server");

    // The launcher's own record travels on stderr, which the confined child
    // inherits, and shared §7's checking rule is that the `info!` half is
    // asserted against it as it stands: the profile, the pre-flight, the spawn
    // with its pid, and the exit. The drain runs on a thread of its own so a
    // full pipe can never block the process under test.
    let mut stderr = child.stderr.take().expect("stderr");
    let logged = thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });

    // The endpoint arrives as an event, not as a line to scrape (decision 19):
    // the confined child emits it and the launcher relays it, so a consumer
    // never has to care which process wrote it. The read is on a thread for the
    // reason `common::wait_for_listening` gives — a startup path that neither
    // reports nor exits would otherwise hang the leg with no test named, libtest
    // having no per-test timeout — and the pump hands over **raw lines**, because
    // the frames-only assertion below is about what did *not* decode, which a
    // pump that decoded in its reader thread could not make
    // (`common::stdout_lines`, and `common::wait_for_listening_in`, which is the
    // shared readiness read over the same stream).
    let lines = stdout_lines(child.stdout.take().expect("stdout"));
    // A refusal and a silence are both killed-and-reaped through `fail`, never a
    // bare panic: a confined server left alive holds its listener and its state
    // lock for the rest of the leg.
    let addr = match wait_for_listening_in(&lines) {
        Ok(addr) => addr,
        // **A refusal is explained before it is reported, and the explanation is
        // the only thing that tells a confined runner from a broken product.**
        // The refusal below already carries `sandbox-exec`'s own diagnostic, so
        // [`explain_refusal`] reads the condition off what arrived instead of
        // probing the environment a second time (see it for the signal). The
        // assertion that follows is unchanged: this leg fails here either way.
        Err(message) => {
            let message = explain_refusal(&message);
            fail(&mut child, logged, &root, &message);
        }
    };

    // **The probe is an induction, not this leg's claim.** That a confined run serves is
    // `confinement.feature`'s G8 (a signed S3 journey through the same binary); what the request
    // below exists for is the access log it makes the confined child write. A request that gets no
    // answer at all still goes through `fail` — the child is not serving, and that is the case
    // where it is least likely to exit on its own.
    if let Err(message) = request(addr) {
        fail(&mut child, logged, &root, &message);
    }

    // A write through the profile: the access log lives in the state directory,
    // `.tinio` under the served root in a read-write run, and only the confined
    // child's own grants allow it. The wait is bounded rather than immediate —
    // the response the client read and the log line the child writes are two
    // events with no ordering between them.
    let access_log = root.join(".tinio/access.log");
    // **The wait is the shared device's; the `is_file` is this leg's own.** The
    // device answers "it is there", and what this leg needs is that the child
    // wrote a *file*: a directory at that path is not the write being waited for,
    // and the failure below has to be the one that says so.
    if !wait_for_path(&access_log, wait_timeout(), POLL) || !access_log.is_file() {
        fail(
            &mut child,
            logged,
            &root,
            &format!(
                "the confined child wrote no access log at {}",
                access_log.display()
            ),
        );
    }

    // **The launcher published `state`, and this leg is the one that would have
    // caught its absence.** A confined macOS server is discoverable through
    // exactly one file: the token that guards `/status` and `/stop` is minted by
    // the launcher and written nowhere else, and `control_name` is the only route
    // to the channel the child serves on. Until the backend wired the relay's
    // readiness announcement the launcher published nothing —
    // `Child::wait_for_ready` answered `None` at once — and measured on macOS
    // 15.8, 2026-09-25, the state directory held `.tinio.toml`, `access.log`,
    // `meta.redb` and the socket, and no `state`: `tinio status` answered
    // `stopped` with "no server has run for this root". Once the file landed it
    // still answered `stopped` — "it is stale" — because the child had never
    // received the token, which is why this leg asks a client command rather than
    // only reading the file. Every assertion above is true of both of those runs —
    // the `listening` frame and the access log — which is why this is a different leg
    // rather than a stronger form of one of them.
    let state_file = root.join(".tinio/state");
    if !wait_for_path(&state_file, wait_timeout(), POLL) || !state_file.is_file() {
        fail(
            &mut child,
            logged,
            &root,
            &format!(
                "the launcher published no `state` at {}",
                state_file.display()
            ),
        );
    }
    let socket = match control_socket(&root) {
        Ok(socket) => socket,
        Err(message) => fail(&mut child, logged, &root, &message),
    };
    // **The comparison is on resolved paths, and that is macOS's doing rather than
    // the file's.** The launcher binds the channel through
    // `transport::resolve_socket_path`, so `control_name` carries the canonical
    // spelling — `/private/tmp/…` — while this leg's root is written `/tmp/…`,
    // which is a symlink to it. Both name the one socket on disk, and the
    // canonical form is what the child's plane resolves from the same definer.
    let bound = fs::canonicalize(&socket).unwrap_or_else(|_| socket.clone());
    let bound_name = bound.to_string_lossy().into_owned();
    // **The file is parsed, never quoted: it is the one document that carries the
    // bearer token.** `state` is `State` serialized with every field — `token`
    // among them (`tinio_api::state`) — and the redaction rule is that type's
    // hand-written `Debug`, which reading the file as text would bypass. Both
    // messages below quote one named field, so a failing leg prints the channel's
    // name or the launcher's pid to the CI log and the operator's terminal, and
    // never the file. The parse is `State`'s own, so what the file says and what
    // this leg reads back cannot drift.
    let published = match State::read(&root.join(".tinio")) {
        Ok(Some(published)) => published,
        // The file was seen above, so absence here is a race this leg cannot act
        // on, and an unreadable file is the launcher's own error text. Both go
        // through `fail` rather than a bare panic, for the reason every other
        // failure path here does: the confined child has to die first.
        Ok(None) => fail(
            &mut child,
            logged,
            &root,
            "the launcher's `state` vanished before it could be read",
        ),
        Err(err) => fail(
            &mut child,
            logged,
            &root,
            &format!("the launcher's `state` could not be read: {err}"),
        ),
    };
    if published.control_name != bound_name {
        fail(
            &mut child,
            logged,
            &root,
            &format!(
                "`state` must name the socket the child bound ({bound_name}): it names {}",
                published.control_name
            ),
        );
    }
    // The pid is the launcher's, which is the process this leg spawned: the
    // `--daemon` parent's readiness check compares the two, so a file naming the
    // confined grandchild instead would make a daemon time out on a run that
    // started perfectly. Read once rather than inside the `fail` calls below,
    // where `child` is borrowed mutably and `child.id()` would be a second,
    // conflicting borrow.
    let launcher_pid = child.id();
    if published.pid != launcher_pid {
        fail(
            &mut child,
            logged,
            &root,
            &format!(
                "`state` must name the launcher's pid ({launcher_pid}): it names {}",
                published.pid
            ),
        );
    }
    // **And a client command reads it, which is the half that proves the token
    // travelled.** `status` probes the control channel with the token the file
    // carries, so `running` is only reachable when the child's plane verifies the
    // *same* token — a state file whose token the child never adopted renders
    // `stopped` with "it is stale", which is exactly what this run did while the
    // frame still serialized the payload the plan carried (`token: None`) instead
    // of the launcher's (measured on macOS 15.8, 2026-09-25). The report's own
    // fields are the **serving** process's — the child answers `/status`, so its
    // `pid` is the child's and not the launcher's the file above names — which is
    // why this asserts the shape of the answer rather than a second pid.
    let mut command = Command::new(tinio_bin());
    command
        .args(["status", root.to_str().expect("utf-8 root")])
        .env("NO_COLOR", "1");
    let reported = job::output(&mut command).expect("run tinio status");
    let reported = String::from_utf8_lossy(&reported.stdout).into_owned();
    if !reported.starts_with("running") || reported.contains("it is stale") {
        fail(
            &mut child,
            logged,
            &root,
            &format!("`tinio status` must discover the running server:\n{reported}"),
        );
    }

    // The relay (design §3 step 9, decision 20 — *not* exit-status forwarding):
    // the signal goes to the **launcher**, which must pass it on to the confined
    // child rather than dying and leaving the child to be killed at a grace
    // period. The child is what we then watch exit; this assertion is the only
    // one in the plan that can tell a relay apart from no relay at all.
    let child_pid = match wait_for_child_pid(child.id()) {
        Some(pid) => pid,
        None => fail(
            &mut child,
            logged,
            &root,
            "the launcher never showed a child process",
        ),
    };
    let mut command = Command::new("kill");
    command.args(["-TERM", &child.id().to_string()]);
    let mut killer = job::spawn(&mut command).expect("kill");
    let signalled = killer.wait().expect("kill");
    if !signalled.success() {
        fail(
            &mut child,
            logged,
            &root,
            &format!("signalling the launcher failed: {signalled:?}"),
        );
    }
    if !waits_for_exit(child_pid) {
        fail(
            &mut child,
            logged,
            &root,
            "the confined child outlived a signal sent to the launcher — no relay, or a dead \
             launcher",
        );
    }

    // **What the launcher's own status *is* is `confinement.feature`'s P2 now** — that the launcher
    // answers with the code its child left, on both launcher platforms — so this leg only waits for
    // the launcher to end: the wait is what reaps it, and what the assertions above must not
    // outlive. A launcher that does not exit at all is [`fail`]'s: it is still running, and on this
    // platform the confined grandchild outlives it. (The child's own ending is still this file's:
    // the `step="exit"` record asserted with the other launcher events below.)
    if wait_for_exit(&mut child).is_none() {
        fail(
            &mut child,
            logged,
            &root,
            "the launcher did not exit after its child was signalled",
        );
    }

    // Shared §7's `info!` half, read off the stderr the child inherited: the
    // events that answer "was this process confined, and to what". A launch that
    // leaves no such record is the failure the requirement exists to prevent,
    // and this is the assertion that keeps it from coming back.
    let logged = logged.join().expect("the stderr drain");
    for step in ["profile", "spawn", "exit"] {
        assert!(
            logged.contains(&format!("step=\"{step}\"")),
            "the launcher logged no `{step}` event:\n{logged}"
        );
    }
    // **The mechanism's own check, and which check it is follows the mechanism**
    // (design decision 6): `sandbox-exec` proves the text with a throwaway helper
    // (`step="preflight"`), and the in-process mechanism proves it by forking an
    // unconfined child that applies it (`step="sandbox-init validator"`). A run is
    // proved by exactly one of them, so the assertion is that one of the two
    // landed rather than that a particular one did — and the resolved mechanism is
    // what says which. Both legs are run by the crate's macOS suite (this file's
    // `MECHANISM`-driven e2e and the crate's own validator unit tests).
    //
    // **For the helper's mechanism that means a field and not the step's name**, and the
    // distinction is the whole of plan §7's D2. `step="preflight"` is emitted three ways
    // (`crates/tinio-sandbox-macos/src/launch.rs`): the skip, `verdict="skipped"`, which is what
    // a **release build** (or a level below `debug`) writes and in which *no profile is compiled
    // at all*; the acceptance, `rules=<n>`; and a refusal, `exit=… diagnostic=…`, which fails the
    // launch before this point. Only the skip can satisfy a step-name assertion without a check
    // having run — so a leg meant to show the pre-flight ran would pass on a build where it never
    // did, in the direction that hides a defect. The string below is therefore the acceptance's
    // **own field**: a skip carries no `rules=` and an acceptance carries no `verdict=`, so
    // neither record can be read as the other.
    //
    // **What that leaves unexercised, stated rather than implied**: this leg runs under the
    // profile cargo builds by default (a debug build, where `cfg!(debug_assertions)` makes the
    // pre-flight unconditional), so the arm this assertion now excludes — the *skipped* verdict —
    // is the release-profile run the plan records as a debt. Nothing here induces a skip.
    let mechanism = env::var("TINIO_SANDBOX_MACOS_MECHANISM").unwrap_or_default();
    let (expected_check, expected_event) = if mechanism == "sandbox-init" {
        (
            "the in-process mechanism",
            "step=\"sandbox-init validator\"",
        )
    } else {
        ("sandbox-exec", "step=\"preflight\" rules=")
    };
    // The skip's own field, asserted **absent** beside the acceptance's — the other half of the
    // discrimination above, and the arm the plan's debt names. It is vacuous under
    // `sandbox-init`, which emits no `step="preflight"` record at all; under the helper it is what
    // makes a release-profile run fail here rather than pass on a check that never ran.
    assert!(
        !logged.contains("step=\"preflight\" verdict="),
        "the pre-flight was skipped, so no profile was compiled and this leg's subject never ran \
         — a release build, or a log level below `debug`:\n{logged}"
    );
    assert!(
        logged.contains(expected_event),
        "the launcher logged no `{expected_event}` event for {expected_check}:\n{logged}"
    );
    assert!(
        logged.contains("step=\"mechanism\""),
        "the launcher never logged which mechanism it resolved:\n{logged}"
    );

    // The event relay's whole job (design §12): what reaches the consumer is
    // frames, and only frames. The child has exited, so this drains to EOF.
    // (The positive half — a stray line the child prints being *dropped* rather
    // than forwarded — is the relay's own unit test in the core; duplicating it
    // here would test the decoder, not the launcher.)
    let mut events = 0usize;
    let mut frame = String::new();
    for line in lines.iter() {
        if line.trim_end().is_empty() {
            if matches!(
                frame_line(&mut frame, &line, Event::decode),
                Some(Event::Listening { .. })
            ) {
                events += 1;
            }
            continue;
        }
        assert!(
            line.starts_with("event: ") || line.starts_with("data: "),
            "a non-frame line reached the consumer: {line:?}"
        );
        frame_line(&mut frame, &line, Event::decode);
    }
    // A frame is an `event:` line, a `data:` line and a blank line — so a
    // *second* legitimate frame's `event:` line is part of the protocol, and
    // counting it as a stray write would be the test failing on correct output.
    // What the count asserts is the number of frames, not of lines.
    assert!(
        events <= 1,
        "a confined server emitted {events} frames; only `Listening` was expected"
    );

    // The leg's own root goes with it: nothing else drops it (see the root's
    // comment), and a shared runner should not accumulate one per run.
    cleanup(&root);
}

/// What the kernel answers when a process that is already inside a Seatbelt
/// profile asks for a second one.
///
/// **This is not a new signal: it is the string `sandbox-exec` itself writes**,
/// measured on macOS 15.8 (2026-09-25) with the outer profile both permissive
/// and `(deny default)`:
///
/// ```text
/// sandbox-exec: sandbox_apply: Operation not permitted     (exit 71)
/// ```
///
/// The launcher's refusal carries it verbatim — `preflight` renders its own
/// stderr as `exit Some(71): {diagnostic}` — so a leg that reads its refusal has
/// already read this, and no second process has to be spawned to find it out.
/// The design records the same fact (its evidence table, "A sandboxed process
/// cannot apply a second sandbox"), which is why a launcher has to stay outside
/// the profile it applies. [`sandbox_refused_to_nest`] is the predicate.
const NESTED_REFUSAL: &str = "sandbox_apply: Operation not permitted";

/// Explain the launcher's refusal, when `sandbox-exec` is what refused and the
/// reason is that the runner is confined already.
///
/// **The diagnostic is the whole point of this function, and it does not exempt
/// anything.** The leg keeps failing — a tester who sees red must not read it as
/// a product defect when what broke is the runner's environment, and a product
/// whose confined server starts exactly as it does everywhere else leaves this
/// exact refusal. Anything else is returned untouched: a profile the binary
/// renders wrongly, a missing `sandbox-exec`, an exposure the kernel will not
/// grant, and every timeout all keep their own message and must not be excused
/// as the environment's doing.
///
/// **Two sentences, because a reader arrives with two questions.** The first
/// names the mechanism and the fact that the failure is the environment's rather
/// than the product's; the second says what to do about it and, explicitly, that
/// failing rather than skipping here is this leg's standing design rather than
/// something this diagnostic introduced — the header's "it fails rather than
/// skips" and `docs/tests.md`'s rule for the macOS leg.
fn explain_refusal(message: &str) -> String {
    if !sandbox_refused_to_nest(message) {
        return message.to_string();
    }
    format!(
        "{message}\n\n\
         the test runner is already inside a Seatbelt confinement, and Seatbelt cannot nest: the \
         kernel refuses `sandbox_apply` when a process that is already confined asks for a second \
         profile, so the confined server under test could not be started. This is an environment \
         limitation rather than a product failure — the same binary confines and serves on a host \
         that is not itself confined. The leg fails rather than skips by design (its header); run it \
         from a runner outside any `sandbox-exec`, and nothing in the product needs to change."
    )
}

/// Whether `diagnostic` is `sandbox-exec` refusing to apply a second profile.
///
/// The predicate is the `sandbox-exec` line itself rather than the exit code
/// beside it: `71` is what a refused exec also answers, so the code alone would
/// read a missing exec target as a nested runner. What the predicate deliberately
/// does not claim is *why* the apply was refused — `sandbox_apply` is reached
/// after `sandbox-exec` has already exec'd the target, so on a host whose
/// `sandbox-exec` works at all, a process that is already inside a profile is what
/// refuses it. That is the reading the design measured, and the only one this
/// message needs.
fn sandbox_refused_to_nest(diagnostic: &str) -> bool {
    diagnostic.contains(NESTED_REFUSAL)
}

/// The control socket the launcher bound for the confined child: the one `.sock`
/// file inside the leg's state directory.
///
/// Its name is derived from the canonical root (`tinio-<16 hex>.sock`), so this
/// leg reads it off the directory rather than recomputing it — a second
/// derivation here would be a second definition of the name `state` is asserted
/// against, which is exactly the pair the assertion must keep apart.
///
/// `Err` carries the message rather than panicking for [`fail`]'s reason: this is
/// called while the confined server is alive, and a helper that panicked here
/// would leave the process under test holding its listener and its state lock.
fn control_socket(root: &Path) -> Result<PathBuf, String> {
    let state_dir = root.join(".tinio");
    let mut sockets: Vec<PathBuf> = fs::read_dir(&state_dir)
        .map_err(|err| format!("read {}: {err}", state_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sock"))
        .collect();
    sockets.sort();
    if sockets.len() != 1 {
        return Err(format!(
            "one control socket in {} expected, found {sockets:?}",
            state_dir.display()
        ));
    }
    Ok(sockets.remove(0))
}

/// Best-effort removal of the leg's served root.
///
/// The root is one this leg made under `/tmp` rather than a
/// `tempfile::TempDir` (see its comment), so nothing drops it for us: without
/// this a shared runner accumulates one root per e2e run. A removal that fails
/// is not a test failure — the leg's subject is the confinement — so the result
/// is discarded, and `cleanup` is called on the success path and from every
/// [`fail`], which is where a failing leg leaves its wreckage.
fn cleanup(root: &Path) {
    let _ = fs::remove_dir_all(root);
}

/// Kill and reap the child, drop the leg's root, then fail with `message` and
/// the record it left.
///
/// Killing first is not tidiness: a confined server left alive holds its
/// listener and its state lock for the life of the test process, and the leg
/// that failed is the one whose child is least likely to exit on its own.
/// Reaping is also what ends the stderr drain — whose pipe only reaches EOF at
/// the child's death — so the log quoted below is complete.
///
/// **Every failure in this leg that can leave the child running comes through
/// here**, and that is the rule rather than the habit of a few call sites: the
/// readiness handshake, the probe request, the control-socket read, the
/// child-pid wait, the relay's two assertions and the exit wait all hand this
/// function a message instead of panicking, because a panic is the one shape
/// that cannot kill first.
///
/// **The confined grandchild has to die too, and it does not die with its
/// launcher.** macOS has no job object (decision 20's accepted gap), and the
/// grandchild **inherits the launcher's stderr** — the very pipe the drain above
/// reads — so killing the launcher alone leaves the write end open and the join
/// below blocks for ever. That is not hypothetical: the first version of this
/// leg's `state` assertions failed here and the leg hung instead of reporting
/// (measured on macOS 15.8, 2026-09-25). The leg's root is unique to it — it
/// carries the test process's pid — and names the server in its own argv, so the
/// pattern below reaches both processes; the message is printed before the join so
/// a hang can never be the only evidence again.
fn fail(child: &mut Child, logged: thread::JoinHandle<String>, root: &Path, message: &str) -> ! {
    // The sweep is `common::sweep_confined_grandchild`'s: it owns the pattern and the reason both
    // processes have to go (the launcher, and the confined grandchild it cannot take with it).
    common::sweep_confined_grandchild(root);
    kill_and_reap(child);
    cleanup(root);
    eprintln!("sandbox_macos_e2e: {message}");
    let log = logged.join().unwrap_or_default();
    panic!("{message}; the launcher's stderr:\n{log}");
}

/// Whether `pid` is gone within the suite's readiness budget.
fn waits_for_exit(pid: u32) -> bool {
    let deadline = Instant::now() + wait_timeout();
    while Instant::now() < deadline {
        let mut command = Command::new("kill");
        command.args(["-0", &pid.to_string()]);
        let alive = job::spawn(&mut command)
            .and_then(|mut child| child.wait())
            .map(|status| status.success())
            .unwrap_or(false);
        if !alive {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

/// `pgrep -P` for the confined child; `sandbox-exec` execs in place, so the pid
/// it reports is the one the launcher's relay addresses and the one this test
/// watches for death.
///
/// `None` rather than a panic, again for [`fail`]'s reason: no child in the
/// launcher's process tree is a failure the caller reports with the launcher
/// killed and its record quoted, not a panic that leaves it running.
fn wait_for_child_pid(parent: u32) -> Option<u32> {
    let parent = parent.to_string();
    let deadline = Instant::now() + wait_timeout();
    while Instant::now() < deadline {
        let mut command = Command::new("pgrep");
        command.args(["-P", &parent]);
        let listed = job::output(&mut command);
        // One let-chain rather than three nested `if`s: `clippy::collapsible_if`
        // refuses the nest under `-D warnings`, and edition 2024 is where the
        // chain is available.
        if let Ok(out) = listed
            && let Some(pid) = String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .next()
            && let Ok(pid) = pid.parse()
        {
            return Some(pid);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

/// The launcher's status, when it exits inside the suite's readiness budget;
/// `None` when the deadline passes with it still running.
///
/// **The poll is `tinio_util::testing::poll_exit`'s and the ending is this
/// leg's**, which is why this wrapper still exists: the launcher is still running
/// when the deadline passes, its confined grandchild outlives it (no job object,
/// decision 20's accepted gap) and the kill that reaches both is [`fail`]'s sweep
/// over the leg's root. A panicking wait (`wait_for_exit`, the same module) cannot
/// reach the grandchild, so the deadline hands back `None` and the caller fails.
fn wait_for_exit(child: &mut Child) -> Option<ExitStatus> {
    poll_exit(child, wait_timeout())
}

/// One plain HTTP request, made for its **side effect**: what the leg reads afterwards is the
/// access log line the plane writes for it, and the serving claim itself is `G8`'s (see the
/// module doc). The response is therefore discarded — a request that gets no answer is still a
/// failure, because the assertion below it would then be waiting on a run that never served.
///
/// `Err` rather than a panic for [`fail`]'s reason: every failure here happens
/// while the confined child is alive, and a helper that panicked would leave it
/// holding its listener and its state lock for the rest of the test process.
fn request(addr: SocketAddr) -> Result<(), String> {
    // The probe and its read are `common::answer`'s — one spelling of the request bytes for every
    // leg in this crate; only the message is this leg's, because it names the confined server.
    common::answer(addr)
        .map(|_| ())
        .map_err(|err| format!("the confined server at {addr}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal a confined runner produces, pasted from the run itself rather
    /// than synthesized: this is `common::wait_for_listening_in`'s `Err` on macOS
    /// 15.8 (2026-09-25) with the leg started under
    /// `sandbox-exec -p '(version 1)(allow default)'`, and it is the one input the
    /// nested explanation exists for. Quoting it whole is what keeps this test
    /// measuring the product's rendering instead of a message this file invented.
    ///
    /// **A dated record, and left as pasted.** The `step` it names is
    /// `sandbox-exec -p`, which is what the pre-flight logged on the day it was
    /// captured; the step was renamed to `preflight` afterwards, and that is the
    /// name the live product renders and the one this file asserts `step="preflight"`
    /// for. Rewriting the text would stop it being the string the run produced,
    /// which is the whole reason it is quoted here — and the explanation reads the
    /// `sandbox_apply` line rather than the step name, so the record still exercises
    /// exactly what [NESTED_REFUSAL] exists for.
    const NESTED: &str = "serve refused to start: sandbox error: sandbox step `sandbox-exec -p` \
        failed: exit Some(71): sandbox-exec: sandbox_apply: Operation not permitted";

    /// The diagnostic is reachable on the refusal path, and it brings the two
    /// sentences the operator needs.
    ///
    /// The assertions are split at the message's own paragraph break so neither
    /// can be satisfied by the explanation's text alone: the head is the refusal
    /// as it arrived — the product's own rendering, which the diagnostic only
    /// appends to — and only the tail is allowed to explain it. A refusal whose
    /// own text were dropped, and an explanation that quietly replaced it, both
    /// fail here.
    #[test]
    fn a_nested_refusal_is_explained_as_the_environment_and_not_the_product() {
        let explained = explain_refusal(NESTED);
        let (head, tail) = explained
            .split_once("\n\n")
            .expect("the explanation is its own paragraph");
        assert!(
            head.contains(NESTED_REFUSAL),
            "the refusal this run produced is kept, and it is where the signal is read from: {head:?}"
        );
        assert_eq!(head, NESTED, "the diagnostic appends and never rewrites");
        for sentence in [
            "the test runner is already inside a Seatbelt confinement",
            "Seatbelt cannot nest",
            "an environment limitation rather than a product failure",
        ] {
            assert!(tail.contains(sentence), "`{sentence}` is missing: {tail:?}");
        }
    }

    /// Every other refusal keeps its own message: the diagnostic narrows, it does
    /// not excuse.
    ///
    /// `71` is what a refused exec answers as well, so the exit code beside the
    /// diagnostic cannot be the predicate — the second case below is that refusal
    /// with the sandbox-exec code and nothing else in common, and it is the one
    /// that would be misread as the environment's doing by a looser test.
    ///
    /// Both strings are in [NESTED]'s dated shape: the `step` they name is the
    /// pre-flight's 2026-09-25 spelling, `sandbox-exec -p`, and not the live
    /// `preflight` — which is why neither of them can be the nested one, the
    /// predicate being the `sandbox_apply` line rather than the step name.
    #[test]
    fn a_refusal_that_is_not_the_nested_one_is_returned_untouched() {
        let profile_refused = "serve refused to start: sandbox error: sandbox step `sandbox-exec -p` \
            failed: exit Some(65): invalid data type of path filter";
        let exec_refused = "serve refused to start: sandbox error: sandbox step `sandbox-exec -p` \
            failed: exit Some(71): sandbox-exec: execvp() of '/srv/bin/tinio' failed: Operation not permitted";
        for refusal in [profile_refused, exec_refused] {
            assert_eq!(explain_refusal(refusal), refusal);
        }
    }
}
