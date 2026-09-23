//! Termination signals, turned into the one shutdown channel (design decision 20).
//!
//! `signal-hook` registers and owns the self-pipe on unix; nothing in the handler allocates
//! or locks. On Windows `signal-hook`'s CRT-backed support cannot express the console-close
//! event the contract requires, so the console handler is `windows-sys`'s.
//!
//! [`install`] is called by [`start::run`](crate::commands::start::run) **before** the
//! process's runtime exists (§1 step j before step k), and the pair it answers is the shape
//! the serving halves are written against: [`Handles::shutdown`] is what `serve` takes as its
//! `stop`, and [`Handles::stopper`] is what `Plane::bind` takes, so a termination signal, a
//! `POST /stop` and the data plane's drain all end at the same value.

use std::io;
#[cfg(unix)]
use std::{process, thread};
#[cfg(windows)]
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(unix)]
use signal_hook::{
    consts::{SIGHUP, SIGINT, SIGTERM},
    iterator::Signals,
};
use tokio::sync::watch;
#[cfg(windows)]
use windows_sys::{
    Win32::{
        Foundation::{FALSE, TRUE},
        System::Console::{
            CTRL_BREAK_EVENT, CTRL_C_EVENT, CTRL_CLOSE_EVENT, SetConsoleCtrlHandler,
        },
    },
    core::BOOL,
};

use crate::Error;

/// The process's one shutdown channel, in its two halves.
///
/// **One channel, three entry points** (decision 12): a termination signal, a `POST /stop` and
/// the data plane's drain all end at the same `stop` value, so there is no second way to shut
/// a server down. [`shutdown`](Self::shutdown) is what the serving half waits on — the data
/// plane, the api plane and the background passes each hold a clone — and
/// [`stopper`](Self::stopper) is what `Plane::bind` takes, because `/stop` sends on the very
/// channel a signal flips.
///
/// The `stopper` half is kept alive even where nothing sends on it (an `api`-off build has no
/// plane), and that is a requirement rather than an oversight: a `watch::Receiver` whose every
/// sender has dropped resolves `changed()` with an error at once, so a `Handles` that dropped
/// its sender would shut the serving half down at start-up.
pub struct Handles {
    /// What the serving half waits on.
    pub shutdown: watch::Receiver<bool>,
    /// The plane's `/stop` sender.
    ///
    /// `allow(dead_code)` **in the api-off build only**, and for a reason that is not a
    /// formality: nothing sends on this channel there (there is no plane), but the value must
    /// still be held — the serving halves wait on `shutdown`, and a `watch` receiver whose
    /// every sender has dropped wakes at once, so a `Handles` that did not keep its sender
    /// would shut the server down at start-up. Held is not read, and the allow is the honest
    /// spelling of that rather than a pretend read.
    #[cfg_attr(not(feature = "api"), allow(dead_code))]
    pub stopper: watch::Sender<bool>,
}

/// Installs the termination handlers and answers the shutdown channel.
///
/// `SIGTERM`, `SIGINT` and `SIGHUP` are registered through `signal-hook`'s iterator, whose
/// handler only writes a byte into the crate's self-pipe: the channel is flipped on the
/// ordinary thread draining that pipe, never on the signal path, so no handler of ours
/// allocates, logs or takes a lock. That thread counts the termination signals itself and
/// answers the second with `second_signal_exits_immediately`; `SIGHUP` is logged and ignored,
/// never a shutdown. On Windows the console handler is registered instead
/// (`install_console_handler`), because the console-close event the contract requires is not
/// expressible through `signal-hook`'s CRT-backed support — and it counts its events the same
/// way, so the second one exits the process without draining there too.
///
/// # Errors
///
/// When a handler cannot be registered: `signal-hook`'s own registration failure on unix, a
/// refused `SetConsoleCtrlHandler` on Windows, or — Windows only — a **second** call in one
/// process, because a console handler is a plain `fn` Windows hands no context to and the
/// process has exactly one channel for it to flip.
pub fn install() -> Result<Handles, Error> {
    let (stopper, shutdown) = watch::channel(false);

    #[cfg(unix)]
    {
        // **No `flag::register` here, and the reason is the counter below.** A
        // `flag::register(SIGTERM, Arc<AtomicBool>)` sets its atomic *from the handler*, and
        // the iterator only yields a signal after the handler for that signal has run — so a
        // thread that asked the same atomic "is this the second one?" would read `true` on the
        // **first** signal and exit before ever sending on the channel. The termination count
        // is therefore this thread's own, and the atomic is not registered at all.
        //
        // `stopper` is cloned **before** the `move` closure: the thread takes the clone and the
        // `Handles` returned below keeps the original. Without this line the sender is moved
        // into the thread and the return is an E0382 — the Windows arm never hits it because it
        // hands its own `stopper.clone()` to the registration call.
        let stopper = stopper.clone();
        let mut signals = Signals::new([SIGTERM, SIGINT, SIGHUP]).map_err(signal_error)?;
        thread::spawn(move || {
            let mut terminations = 0u32;
            for signal in signals.forever() {
                if signal == SIGHUP {
                    // The contract: SIGHUP is ignored and logged, never a shutdown. The
                    // iterator thread is where "logged" can happen — a handler cannot log.
                    tracing::info!(signal, "SIGHUP ignored (no config reload in v1)");
                    continue;
                }
                terminations += 1;
                if terminations > 1 {
                    second_signal_exits_immediately();
                    continue;
                }
                tracing::info!(signal, "shutting down");
                let _ = stopper.send(true);
            }
        });
    }

    #[cfg(windows)]
    install_console_handler(stopper.clone())?;

    Ok(Handles { shutdown, stopper })
}

/// The fast-exit arm: a second termination signal exits immediately, without draining
/// (`contracts/cli.md:43`).
///
/// Called only from the iterator thread, on its second count, and unix-only like that caller:
/// Windows answers its second console event by **declining** it (`install_console_handler`
/// returns `FALSE` there), so the process is terminated by the default handler instead of by
/// an exit call of ours running on the system's thread.
#[cfg(unix)]
fn second_signal_exits_immediately() {
    tracing::warn!("second termination signal: exiting now");
    process::exit(0);
}

/// The one failure both arms report: a handler that could not be installed.
fn signal_error(err: io::Error) -> Error {
    Error::Operational(format!("the signal handlers could not be installed: {err}"))
}

/// The sender the console handler reaches the shutdown channel through.
///
/// A console handler is a plain `fn` — Windows hands it no context — so the channel it flips
/// has to be reachable from a module-level value. `install_console_handler` sets it **before**
/// the registration, and it is never dropped: this sender is also what keeps the channel open
/// for the process's whole life.
#[cfg(windows)]
static STOPPER: OnceLock<watch::Sender<bool>> = OnceLock::new();

/// The process's termination events, counted: the first is the graceful one, the second is
/// the contract's fast exit.
#[cfg(windows)]
static TERMINATIONS: AtomicU32 = AtomicU32::new(0);

/// Whether this call is the process's **first** termination event.
///
/// One relaxed atomic increment — lock-free and allocation-free, which is what allows it to
/// run on the thread the system creates for the handler. It is a function of its own, apart
/// from the handler's `unsafe` body, so that the counting the second signal turns on is
/// testable without this crate declaring a second `unsafe` site.
#[cfg(windows)]
fn first_termination_event() -> bool {
    TERMINATIONS.fetch_add(1, Ordering::Relaxed) == 0
}

/// The bounded wait a console-close event gives the drain: **three seconds**.
///
/// The bound is Windows's rather than this design's. `HandlerRoutine`'s own timeout table
/// gives `CTRL_CLOSE_EVENT` 5000 ms (`SPI_GETHUNGAPPTIMEOUT`) and terminates the process when
/// the handler returns, so the wait happens **inside** the handler and stays well inside the
/// budget it is granted — the drain a closing console can afford is shorter than the one
/// `POST /stop` grants (10 s), and a drain that outlives it is cut off rather than waited for.
#[cfg(windows)]
const CLOSE_DRAIN: Duration = Duration::from_secs(3);

/// Declares the Windows console handler and registers it.
///
/// **One of this crate's three enumerated `#[allow(unsafe_code)]` sites** — the others are
/// `daemon.rs`'s `set_launch_env` and `doctor.rs`'s `space` (the `GetDiskFreeSpaceExW` disk-space
/// probe) — and it holds both halves of the exception — the
/// `unsafe extern "system" fn` a console handler must be and the raw `windows-sys` registration
/// call — in one item, which is why `crates/tinio-cli/Cargo.toml`'s `unsafe_code` is `deny`
/// rather than `forbid` (design decision 20; the same shape `tinio-winsec`'s file DACLs and
/// `tinio-api`'s `dacl` module take). The invariant inside it is Windows's rather than this
/// design's: the handler runs on
/// a thread the system creates, so it may not allocate, format or lock, and the whole of its
/// work is **one atomic increment** (the termination count) and a **`send`** on the channel
/// [`install`] created **before** the handler was registered — the drain itself runs on the
/// serving threads, not there.
///
/// `CTRL_C_EVENT` and `CTRL_BREAK_EVENT` are handled by that `send` alone and answer `TRUE`,
/// which is what leaves the process alive to finish its drain and exit 0: the documentation's
/// timeout table gives both of them no timeout at all. A **second** termination event answers
/// `FALSE` instead, which hands the process to the default handler's immediate termination —
/// the contract's "a second signal exits immediately without draining" (`contracts/cli.md:43`),
/// answered without an exit call of ours running on the system's thread. `CTRL_CLOSE_EVENT`
/// shares the arm and adds `CLOSE_DRAIN`'s bounded wait, because the system terminates the
/// process once a close handler returns. `CTRL_LOGOFF_EVENT` and `CTRL_SHUTDOWN_EVENT` are
/// deliberately not handled: they reach services only, and an interactive application is
/// already gone when the system sends them, so the honest answer is `FALSE` — the next
/// handler's business, not ours.
#[cfg(windows)]
#[allow(unsafe_code)]
fn install_console_handler(stopper: watch::Sender<bool>) -> Result<(), Error> {
    /// The handler Windows runs, on a thread of its own, for every console control event.
    unsafe extern "system" fn on_console_event(event: u32) -> BOOL {
        if !matches!(event, CTRL_C_EVENT | CTRL_BREAK_EVENT | CTRL_CLOSE_EVENT) {
            return FALSE;
        }
        if !first_termination_event() {
            // The process's second termination event: declining it is the whole of the fast
            // exit, and it costs the handler nothing but that increment — `FALSE` lets the
            // default handler terminate the process with no drain and no unwinding of ours.
            return FALSE;
        }
        let Some(stopper) = STOPPER.get() else {
            // Nothing to flip, so nothing is handled: `FALSE` lets the default handler
            // terminate the process, which is what an unhandled console event means.
            // Unreachable while the registration below follows the `set`, and the honest
            // answer even if it did not.
            return FALSE;
        };
        let _ = stopper.send(true);
        if event == CTRL_CLOSE_EVENT {
            thread::sleep(CLOSE_DRAIN);
        }
        TRUE
    }

    if STOPPER.set(stopper).is_err() {
        return Err(Error::Operational(
            "the console handler is already installed in this process".into(),
        ));
    }
    if unsafe { SetConsoleCtrlHandler(Some(on_console_event), TRUE) } == FALSE {
        return Err(signal_error(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use rustix::process::{Signal, getpid, kill_process};
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn a_signal_flips_the_shutdown_channel() {
        let handles = install().expect("install");
        let mut shutdown = handles.shutdown.clone();
        // The test's own process is the only place a handler can be installed, so this test
        // asserts the wiring; the process-level leg is the Windows console-handler case in
        // `crates/tinio/tests/lifecycle.rs` (the `tinio` package's file, where
        // `CARGO_BIN_EXE_tinio` resolves) — no scenario can raise a console control event.
        kill_process(getpid(), Signal::TERM).expect("raise");
        timeout(Duration::from_secs(5), shutdown.changed())
            .await
            .expect("the channel flips")
            .expect("sender alive");
        assert!(*shutdown.borrow());
    }
}

// The Windows half's unit, and the only leg the console handler has on this side: a real
// console event needs a console and a second process, so what is asserted here is the count
// that decides between the graceful first event and the fast-exiting second. The handler
// itself is a `FALSE` away from this count, and the end-to-end leg belongs to
// `crates/tinio/tests/lifecycle.rs` (the `tinio` package's file).
#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn the_second_console_event_is_declined() {
        assert!(
            first_termination_event(),
            "the first event is the graceful one"
        );
        assert!(
            !first_termination_event(),
            "the second event exits without draining"
        );
        assert!(!first_termination_event(), "and so does every one after it");
    }
}
