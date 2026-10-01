//! Relaying termination signals to the confined child while the launcher waits.
//!
//! Design §3 step 9 and decision 20. The obligation is **required on Windows**
//! (a container signals its main process and nothing else, so the launcher
//! receives the stop and the confined server never does) and **defensive on
//! macOS** (`launchd` signals the whole job, and a container on a Mac is a Linux
//! VM running the in-place Linux path). It is built on both because the failure
//! is silent — a missing relay looks exactly like a server that ignores
//! `SIGTERM` — and because it keeps the launcher contract uniform.
//!
//! # Mechanism
//!
//! `signal-hook` installs the handlers and writes one byte per signal into a
//! self-pipe, so **no code of ours runs in a signal handler at all**. A
//! dedicated thread drains that pipe and relays with one `kill` syscall per
//! signal. Everything that has to stay async-signal-safe is on the handler's
//! side of that boundary, and none of it is ours: what this module owns is a
//! normal thread, which may allocate, take a lock, and emit the `debug!` event
//! shared §7 asks for — each relayed signal with its number — after the `kill`
//! returns. An earlier version of this paragraph said the thread "touches
//! exactly two things … and does not log"; the event is the third, and the
//! distinction that matters is the one above rather than the count, because the
//! thread is not the signal path.
//!
//! # The alternative, and why it is not taken
//!
//! A hand-written `sigaction` through FFI is the only other correct option:
//! `rustix` deliberately does not wrap `sigaction` or `sigprocmask` (they sit in
//! its `not_implemented` list, exactly because libc may need to be involved in
//! signal handling).
//!
//! **The strongest argument this plan first made for `signal-hook` is void, and
//! it is better said plainly than left standing.** It argued that the crate
//! keeps `tinio-sandbox-macos` on `unsafe_code = "forbid"`. It does not, and it
//! cannot: the crate is `deny` with enumerated `#[allow(unsafe_code)]` sites
//! (the inherited-descriptor rebuild in `launch`, the libSystem FFI in `apply`,
//! and `apply`'s validator `fork`; design decision 14), because no route to any of
//! them is safe. What remains is a smaller but real argument — a safe crate
//! still beats one more hand-written `unsafe` site under `deny`. A
//! hand-rolled handler is precisely where async-signal-safety bugs live, and
//! taking that route would mean adding the new site to decision 14's list — an
//! explicit, reviewable change that no `#[allow(unsafe_code)]` may make quietly. One small dependency is the cheaper
//! of the two, and its marginal cost is small: `signal-hook-registry`, the crate
//! that actually owns the handler and the self-pipe, is already in the tree
//! transitively. This is a preference between two working options rather than a
//! lint-forced choice, and a maintainer who rejects the dependency must take the
//! lint route explicitly — not read this plan as licence for a quiet
//! `#[allow(unsafe_code)]`.

// Every use below belongs to the crate's one platform, and none of them carries
// a gate of its own: the crate root takes `#![cfg(target_os = "macos")]`, so a
// second gate here would be noise the root already covers.
use std::{
    io,
    sync::{
        OnceLock,
        atomic::{AtomicI32, Ordering},
    },
    thread,
};

// The relaying thread's two members, the kill itself and the signal set, imported rather than
// written as paths: `std::thread::Builder`, `rustix::process::kill_process`,
// `signal_hook::iterator::Signals` and `SIGTERM` are all three segments or more, which
// this crate's `absolute_paths = "warn"` (promoted to an error by `-D warnings`)
// refuses. The names are also the ones the module's own prose uses, and
// `Signals` is imported **once** for the two functions that name it — the same
// import written twice was a review finding rather than a second decision.
use rustix::process::{Pid, Signal, kill_process};
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use tracing::debug;

/// The signals a launcher relays: the termination set a supervisor sends.
///
/// Anything outside this set is not a shutdown request, and forwarding
/// everything would make the launcher a signal-transparent proxy it is not.
///
/// **The names rather than raw numbers, because `signal_hook`'s constants are
/// `const c_int`s** — the same values the relay compares against, spelled once
/// here and once in the module that owns them. The set is still one constant a
/// caller cannot narrow, and the pure decision below is still testable without a
/// signal ever being sent.
pub const RELAYED_SIGNALS: &[i32] = &[SIGTERM, SIGINT];

/// The child's pid while it lives; `0` once the relay is disarmed or the child
/// is gone.
static CHILD_PID: AtomicI32 = AtomicI32::new(0);

/// Whether the handlers and the relaying thread have been started.
static INSTALLED: OnceLock<io::Result<()>> = OnceLock::new();

/// What one received signal should do — plain data, so it is testable without a
/// signal ever being sent: the signal to relay, or `None` when the child is gone
/// or the number is not one this launcher relays.
///
/// **The set is read here rather than passed in.** Every caller there ever was —
/// the relaying thread and each test — handed over [`RELAYED_SIGNALS`] itself, so
/// the parameter was a second spelling of one constant and nothing else; a caller
/// free to pass a narrower set is a caller free to make the launcher stop
/// forwarding a signal it promises, which is the drift this signature removes by
/// construction.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::relay::relay_decision;
///
/// assert_eq!(relay_decision(4242, 15), Some(15));
/// assert_eq!(relay_decision(0, 15), None);
/// ```
pub fn relay_decision(child_pid: i32, raw: i32) -> Option<i32> {
    if child_pid <= 0 || !RELAYED_SIGNALS.contains(&raw) {
        return None;
    }
    Some(raw)
}

/// Install the stop-signal handling for `child_pid`: from here until
/// [`disarm`], a termination
/// signal this process receives is forwarded to the child.
///
/// Called by the launcher **after** the child exists. Before that there is
/// nothing to relay to, and dying on a signal that arrives before the child is
/// the correct behaviour rather than a gap in the relay.
///
/// # Errors
///
/// Whatever installing the handlers or starting the thread reports; startup
/// fails closed on it (design §11). An operator who asked for a supervisor
/// should hear at startup that it cannot relay, rather than during a shutdown.
///
/// # Examples
///
/// ```no_run
/// tinio_sandbox_macos::relay::on_stop(4242)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn on_stop(child_pid: i32) -> io::Result<()> {
    match INSTALLED.get_or_init(start_relaying) {
        Ok(()) => {
            CHILD_PID.store(child_pid, Ordering::SeqCst);
            debug!(
                step = "relay-arm",
                pid = child_pid,
                "termination signals are relayed to the confined child"
            );
            Ok(())
        }
        Err(err) => Err(io::Error::new(err.kind(), err.to_string())),
    }
}

/// Disarm: a signal received after this is relayed nowhere.
///
/// The caller runs this in the teardown that `supervise` already takes, which
/// runs on every exit path (decision 15). It closes part of the pid-reuse
/// window: with no disarm, a signal arriving after the child has been reaped
/// would be forwarded to whatever owns that pid by then. **It cannot close the
/// whole of it, and this doc used to imply that it could** — the disarm runs in
/// teardown, after the wait has returned, so a signal that arrives between the
/// reap and the disarm is still forwarded to a pid the kernel may already have
/// reused. The window is a few instructions wide and no standard deployment
/// reaches it; what would close it is disarming before the reap, which would
/// cost the relay its last signal.
///
/// **`disarm` stops the relaying, and it does not restore `SIGTERM`/`SIGINT` to
/// their default disposition.** The handlers stay installed for the rest of the
/// process's life: this crate registers through `signal-hook`'s iterator API,
/// which owns the registration ids and does not hand them back, so there is no
/// `unregister` for it to call — and the relaying thread is left parked on a
/// self-pipe nobody writes to. The consequence is concrete and worth knowing:
/// after a teardown, this process **swallows** a later `SIGTERM` or `SIGINT`
/// instead of dying of it. `serve` never notices (its teardown is the last thing
/// before `std::process::exit`), and the relay tests rely on it deliberately —
/// the second test sends itself a `SIGTERM` and expects to survive. What it
/// means for a caller that keeps running is that a disarm is not a full
/// restoration; restoring the default would mean registering through
/// `signal_hook::low_level` directly, which is a different design and not this
/// one.
///
/// # Examples
///
/// ```rust
/// tinio_sandbox_macos::relay::disarm();
/// ```
pub fn disarm() {
    CHILD_PID.store(0, Ordering::SeqCst);
    debug!(
        step = "relay-disarm",
        "termination signals are no longer relayed"
    );
}

/// The **child-side** stop hook: run `hook` when the launcher's relay delivers a
/// termination signal to this process.
///
/// This is the other end of design §3 step 9, and it runs in a different process
/// from the one [`on_stop`] arms. The launcher forwards a real `SIGTERM` here, so
/// without a registration of its own the child dies of the default disposition —
/// a legitimate end for a run that asked to stop, but not the graceful shutdown
/// the CLI contract promises. What `hook` does is start that shutdown (Task 8's
/// `confined` hands it a `watch::Sender`), and it runs on the draining thread
/// rather than in a handler, which is the same boundary the mechanism section
/// above describes.
///
/// **The name is not `on_stop` because that one is the launcher's half.** The two
/// halves need two names in this crate; the facade presents this one under the
/// shared name `on_stop`, which is also what a Windows child calls with the same
/// closure shape (`2026-09-18-sandbox-p2-windows.md`).
///
/// # Errors
///
/// Whatever `signal-hook` reports while registering — the same refusal
/// [`on_stop`] propagates, and for the same reason: a child that cannot be told
/// to stop is one that gets killed at the end of a grace period instead of
/// draining.
///
/// # Examples
///
/// ```no_run
/// let (tx, _rx) = std::sync::mpsc::channel();
///
/// tinio_sandbox_macos::relay::on_forwarded_stop(move || {
///     let _ = tx.send(());
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn on_forwarded_stop(hook: impl Fn() + Send + 'static) -> io::Result<()> {
    let mut signals = Signals::new(RELAYED_SIGNALS)?;
    thread::Builder::new()
        .name("tinio-sandbox-stop".into())
        .spawn(move || {
            for raw in signals.forever() {
                debug!(
                    step = "stop-hook",
                    signal = raw,
                    "the confined child received a forwarded stop"
                );
                hook();
            }
        })?;
    Ok(())
}

fn start_relaying() -> io::Result<()> {
    let mut signals = Signals::new(RELAYED_SIGNALS)?;
    thread::Builder::new()
        .name("tinio-sandbox-signals".into())
        .spawn(move || {
            for raw in signals.forever() {
                let child = CHILD_PID.load(Ordering::SeqCst);
                let Some(raw) = relay_decision(child, raw) else {
                    continue;
                };
                let (Some(pid), Some(signal)) = (Pid::from_raw(child), Signal::from_named_raw(raw))
                else {
                    continue;
                };
                // Best effort: a child that has already gone is the normal case at
                // the end of a run.
                let _ = kill_process(pid, signal);
                // After the `kill`, on a normal thread — this is not a signal
                // handler, and `debug!` here is what shared §7 asks for: each
                // relayed signal with its number. The handler's side of the boundary
                // is still signal-hook's own self-pipe write and nothing of ours.
                debug!(
                    step = "signal",
                    pid = child,
                    signal = raw,
                    "relaying a termination signal"
                );
            }
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The relay, measured: a signal sent to the launcher reaches the child, and
    //! the launcher survives it (design §3 step 9, decision 20).
    //!
    //! This is deliberately *not* part of the confined-server test. A launcher with
    //! no relay passes every exit-status assertion there; only a test that signals
    //! the launcher itself can tell the relay apart from the forwarding.
    //!
    //! **These two legs are the lib unit-test binary's, and they change it.** The
    //! handler `on_stop` installs is process-wide and `disarm` leaves it standing,
    //! so a `cargo test -p tinio-sandbox-macos` run swallows SIGTERM/SIGINT from the
    //! first leg onward. CI is unaffected (nextest gives each test its own process);
    //! the local lib target carries it. The 2026-10-01 placement cleanup moved them
    //! here from `tests/relay.rs` and accepted that cost
    //! (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`).

    use std::{
        os::unix::process::ExitStatusExt as _,
        process::{Command, Stdio},
        sync::Mutex,
        thread::sleep,
        time::Duration,
    };

    use rustix::process::getpid;
    use tinio_util::testing::{kill_and_reap, poll_exit};

    use super::*;

    #[test]
    fn a_live_child_is_the_relay_target() {
        // The set itself, not a copy of it: `RELAYED_SIGNALS` is what
        // `relay_decision` reads, so the loop follows the constant wherever it
        // moves — a test that spelled `[15, 2]` by hand would keep passing after
        // the set changed, which is exactly the drift this test exists to catch.
        for raw in RELAYED_SIGNALS {
            assert_eq!(relay_decision(4242, *raw), Some(*raw));
        }
    }

    #[test]
    fn a_disarmed_relay_targets_nothing() {
        // `0` is the disarmed state, and a negative pid means "a process group"
        // to `kill(2)` — neither may ever be a relay target.
        for raw in RELAYED_SIGNALS {
            assert_eq!(relay_decision(0, *raw), None);
            assert_eq!(relay_decision(-1, *raw), None);
        }
    }

    #[test]
    fn only_the_relayed_set_is_forwarded() {
        // Both numbers are outside the set by construction, so a set that grew
        // to include one of them fails here instead of passing quietly:
        // `SIGKILL` cannot be caught or relayed, and `SIGHUP` is not a shutdown
        // request.
        assert!(
            !RELAYED_SIGNALS.contains(&9),
            "SIGKILL cannot be caught or relayed"
        );
        assert!(
            !RELAYED_SIGNALS.contains(&1),
            "SIGHUP is not a shutdown request"
        );
        assert_eq!(relay_decision(4242, 9), None);
        assert_eq!(relay_decision(4242, 1), None);
    }

    /// The two tests below share this module's process-wide state — the armed pid
    /// and the installed handlers — and **libtest runs tests in parallel by
    /// default**, so without this lock they race: the disarmed test's `disarm` can
    /// land between the first test's `on_stop` and its signal, and the first test
    /// then fails for a reason that has nothing to do with the relay.
    static SERIAL: Mutex<()> = Mutex::new(());

    #[test]
    fn a_signal_to_the_launcher_reaches_the_child_and_not_the_launcher() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // A stand-in child that outlives the test unless it is signalled. The real
        // one is `sandbox-exec` and is covered by Task 9's end-to-end run.
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn");
        let pid = child.id() as i32;

        on_stop(pid).expect("install the stop-signal handling");

        // Signal *this* process, the way a supervisor signals the launcher. The
        // handler `on_stop` installed is what keeps this process alive: before the
        // implementation exists, this line kills the test binary, and that is the
        // failure this test is written to show.
        kill_process(getpid(), Signal::TERM).expect("signal the launcher");

        // The shared bounded poll, and this leg's own ending: the child is killed before the panic so
        // a red run leaves nothing behind (`tinio_util::testing::poll_exit`).
        let status = match poll_exit(&mut child, Duration::from_secs(10)) {
            Some(status) => status,
            None => {
                let _ = child.kill();
                panic!("the child outlived the relayed signal");
            }
        };

        // It died of the relayed signal, not of something else.
        assert_eq!(status.signal(), Some(Signal::TERM.as_raw()), "{status:?}");

        disarm();
    }

    #[test]
    fn a_disarmed_relay_leaves_the_child_alone() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn");
        on_stop(child.id() as i32).expect("install");
        disarm();

        // The handler stays installed after the disarm (that is `disarm`'s documented
        // residue), so this line does not kill the test binary — which is what makes
        // the assertion below about the *relay* and not about this process surviving.
        kill_process(getpid(), Signal::TERM).expect("signal the launcher");

        sleep(Duration::from_millis(200));
        assert!(
            child.try_wait().expect("try_wait").is_none(),
            "a disarmed relay must not forward"
        );
        kill_and_reap(&mut child);
    }
}
