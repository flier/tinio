//! Termination signals, turned into the one shutdown channel (design decision 20).
//!
//! On unix `signal-hook` registers the handlers and **this module owns the wake they write**: a
//! socketpair of our own, one `write(2)` per signal, and the thread that drains it — so nothing in
//! a handler allocates, logs, locks **or waits**. That last word is load-bearing: the write end is
//! **non-blocking**, so a wake the socket cannot hold is dropped rather than waited for, and the
//! drain reads a burst in bulk so it is behind for as little of a flood as it can be. The residual
//! is stated where the write happens; the short of it is that a dropped `SIGHUP` is harmless by
//! contract while a termination byte is exposed only while a burst keeps the socket full.
//! **Owning the wake is a fix rather than a style choice**: the
//! library's own self-pipe is woken with `libc::send`, which on Linux *is* the `sendto` syscall,
//! and the confinement's seccomp filter denies that syscall number wholesale while the library
//! drops the failed wake on purpose — so inside a confined run no byte ever landed and `SIGTERM`,
//! `SIGINT` and `SIGHUP` were all silently dead
//! (`crates/tinio-sandbox-linux/src/seccomp.rs`'s module doc is that trap's home, and it is what
//! the next reader inside a confinement has to know: wake with `write(2)`). On Windows
//! `signal-hook`'s CRT-backed support cannot express the console-close event the contract
//! requires, so the console handler is `windows-sys`'s.
//!
//! [`install`] is called by [`start::run`](crate::commands::start::run) **before** the
//! process's runtime exists (§1 step j before step k), and the pair it answers is the shape
//! the serving halves are written against: [`Handles::shutdown`] is what `serve` takes as its
//! `stop`, and [`Handles::stopper`] is what `Plane::bind` takes, so a termination signal, a
//! `POST /stop` and the data plane's drain all end at the same value.

use std::io;
#[cfg(unix)]
use std::{io::Read as _, os::unix::net::UnixStream, process, sync::Arc, thread};
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
    low_level::register,
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
    /// Nothing sends on this channel in an `api`-off build (there is no plane), and the value
    /// must still be held there: the serving halves wait on `shutdown`, and a `watch` receiver
    /// whose every sender has dropped wakes at once, so a `Handles` that did not keep its sender
    /// would shut the server down at start-up. Held is not read — and as a `pub` field of a
    /// `pub` struct it draws no `dead_code` warning, so the exemption this used to carry is gone
    /// rather than restated.
    pub stopper: watch::Sender<bool>,
}

/// Installs the termination handlers and answers the shutdown channel.
///
/// `SIGTERM`, `SIGINT` and `SIGHUP` are registered through `signal-hook`'s low-level registration,
/// whose handler writes **one byte — the signal's own number — into a socketpair this module
/// owns**, with a single **non-blocking** `write(2)`: the channel is flipped on the ordinary thread
/// draining that socket, never on the signal path, so no handler of ours allocates, logs, takes a
/// lock **or waits for space in a full socket**. That thread reads a burst in bulk and counts the
/// termination signals itself, answering the second with
/// `second_signal_exits_immediately`; `SIGHUP` is logged and ignored, never a shutdown. On Windows
/// the console handler is registered instead
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
        // `flag::register(SIGTERM, Arc<AtomicBool>)` sets its atomic *from the handler*, and that
        // handler runs before the wake byte reaches the thread below — so a thread that asked the
        // same atomic "is this the second one?" would read `true` on the **first** signal and exit
        // before ever sending on the channel. The termination count is therefore this thread's
        // own, and the atomic is not registered at all.
        //
        // `stopper` is cloned **before** the `move` closure: the thread takes the clone and the
        // `Handles` returned below keeps the original. Without this line the sender is moved
        // into the thread and the return is an E0382 — the Windows arm never hits it because it
        // hands its own `stopper.clone()` to the registration call.
        let stopper = stopper.clone();
        // **The wake is this module's own, and it is one `write(2)`.** `signal-hook`'s iterator
        // wakes its selector thread with `libc::send`, and on Linux `send(2)` *is* `sendto` — the
        // syscall number the confinement's seccomp filter refuses wholesale, with the error the
        // library drops on purpose. That wake therefore never landed inside a confined run, the
        // selector thread stayed blocked in `recvfrom`, and the signals were silently dead
        // (measured, WSL2 2026-09-30: `sendto(5, "X", 1, MSG_DONTWAIT, NULL, 0) = -1 EPERM`
        // confined against `= 1` unconfined, same binary;
        // `crates/tinio-sandbox-linux/src/seccomp.rs`'s module doc carries the trap). A socketpair
        // of our own, woken by `write(2)`, is permitted where `send` is refused.
        let (mut reader, writer) = UnixStream::pair().map_err(signal_error)?;
        // The write end is shared rather than cloned per signal (`try_clone` would spend three
        // descriptors on one end), and **nothing is written to it here**: the wake happens in the
        // handlers `register_wakes` installs — which is also where the non-blocking flag is set,
        // because that flag *is* the handler's contract rather than this call's convenience.
        register_wakes(&Arc::new(writer))?;
        thread::spawn(move || {
            let mut terminations = 0u32;
            // **A burst is drained in as few reads as its bytes allow**, because a wake past the
            // socketpair's capacity is *dropped*, not buffered: a reader that spends one syscall
            // per byte (and one log line per `SIGHUP`) is a reader a tight `kill -HUP` loop
            // outruns, and every byte dropped while it is behind is a wake the process never sees.
            // **The residual, stated rather than implied**: a dropped `SIGHUP` is harmless by
            // contract (it is ignored whether or not it is seen), and a dropped *termination* byte
            // is the exposure — possible while a sustained burst keeps the socket full, answered by
            // the next termination signal or by `SIGKILL`. That is no loss the wake it replaced did
            // not also take: the library's write dropped its byte when its pipe was full, and its
            // per-signal `AtomicBool` coalesced repeated deliveries of one signal into one, so it
            // too was not signal-for-signal under a burst. What this loop guarantees instead is
            // that every delivery it *sees* is counted — one byte per delivery, in the order the
            // handlers wrote them, which is what `contracts/cli.md`'s fast exit asks for.
            let mut wake = [0u8; 64];
            loop {
                let read = match reader.read(&mut wake) {
                    // A closed socket is the end of the signals this process has left to select,
                    // not a panic: nothing joins this thread.
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                };
                for byte in &wake[..read] {
                    let signal = i32::from(*byte);
                    if signal == SIGHUP {
                        // The contract: SIGHUP is ignored and logged, never a shutdown. The selector
                        // thread is where "logged" can happen — a handler cannot log.
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
            }
        });
    }

    #[cfg(windows)]
    install_console_handler(stopper.clone())?;

    Ok(Handles { shutdown, stopper })
}

/// Registers the three handlers over the caller's write end, each of which wakes the reader with
/// **one byte holding the signal's own number** — and **marks that end non-blocking first**, which
/// is the whole of the guarantee that a handler can never wait.
///
/// A blocking `write(2)` into a full socketpair waits for space, and the thread that has to make
/// that space is the drain `install` spawns — itself one of the threads a signal is delivered to.
/// A burst can therefore block the only reader *inside* a handler and wedge the process for good:
/// measured on this host, an `AF_UNIX` `SOCK_STREAM` pair takes **278** one-byte sends before
/// `EAGAIN` (a 1-byte skb costs ~768 bytes of truesize against `net.core.wmem_default = 212992`),
/// one `kill -HUP` loop fills it, and the blocking draft of this file left every thread
/// (`sock_alloc_send_pskb`) of a confined server wedged with `SIGTERM` ignored for good. `O_NONBLOCK`
/// with the error dropped is exactly what the library's own wake did (`MSG_DONTWAIT`), and what
/// this handler's contract needs. Only the *writer* end: the drain wants its blocking read.
///
/// **A function of its own because it is one of this crate's enumerated `#[expect(unsafe_code)]`
/// sites**, and the shape every other site here takes: one item that holds the exception together
/// with the argument making it sound, rather than an allow spread over the call site (the list's
/// home is [`install_console_handler`]'s doc). It is unsafe at all because `signal-hook`'s
/// registration *is*: the action it takes runs **inside a signal handler**. The flag is set
/// *before* the first registration, so no handler ever runs against a blocking descriptor; taking
/// the `Arc` by reference rather than by value is what lets a caller (the unit leg below) keep the
/// write end and fill the socket without a handler in it.
///
/// # Safety
///
/// The action must be async-signal-safe, must touch no unsynchronized global and must not panic.
/// The closure registered here is one `rustix::io::write` of one byte into a socketpair this
/// process created before the registration: `write(2)` is in POSIX's async-signal-safe set, the
/// writer end is a descriptor the process owns **and is non-blocking** — a full socket answers
/// `EAGAIN`, which the dropped answer swallows, so the handler never waits — the `Arc` dereference
/// reads a pointer nothing
/// mutates (the registration clones it, once per signal, before any handler exists) and the byte
/// is a copy of a `'static` constant. The answer is dropped rather than unwrapped, so a wake the
/// confinement refused cannot unwind out of a handler — and the thread draining that socket is
/// where everything else (the count, the logging, the channel) happens.
#[cfg(unix)]
#[expect(
    unsafe_code,
    reason = "one of this crate's enumerated `unsafe_code` sites: a single async-signal-safe `write(2)` on a descriptor this process owns, captured before registration — allocation-free, lock-free and non-blocking"
)]
fn register_wakes(writer: &Arc<UnixStream>) -> Result<(), Error> {
    use rustix::io::write;

    writer.set_nonblocking(true).map_err(signal_error)?;
    for signal in [SIGTERM, SIGINT, SIGHUP] {
        let writer = Arc::clone(writer);
        // SAFETY: the action is one `write(2)` on a descriptor this process owns, captured before
        // the registration — async-signal-safe, allocation-free, lock-free, panic-free and
        // non-blocking (the writer end was set `O_NONBLOCK` above, before this call), as the doc
        // argues in full.
        unsafe {
            register(signal, move || {
                let _ = write(&*writer, &[signal as u8]);
            })
        }
        .map_err(signal_error)?;
    }
    Ok(())
}

/// The fast-exit arm: a second termination signal exits immediately, without draining
/// (`contracts/cli.md:43`).
///
/// Called only from the selector thread, on its second count, and unix-only like that caller:
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
/// **One of this crate's four enumerated `#[expect(unsafe_code)]` sites** — the others are
/// `daemon.rs`'s `set_launch_env`, `doctor.rs`'s `space` (the `GetDiskFreeSpaceExW` disk-space
/// probe) and [`register_wakes`], the unix registration: `signal-hook`'s registration is `unsafe`
/// because the action it takes runs inside a signal handler, and that function's doc is where the
/// argument for the action registered there lives. This item holds both halves of its own
/// exception — the `unsafe extern "system" fn` a console handler must be and the raw `windows-sys`
/// registration call — in one item, which is why `crates/tinio-cli/Cargo.toml`'s `unsafe_code` is
/// `deny` rather than `forbid` (design decision 20; the same shape `tinio-api`'s `pipe` module takes). The invariant inside it is Windows's rather than this
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
#[expect(
    unsafe_code,
    reason = "the crate's console-handler site, and the head of its enumerated list: Windows runs the handler on a thread of its own, so it must be `unsafe extern \"system\"`, and it only increments a counter and answers `TRUE`/`FALSE` — it never unwinds"
)]
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
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };

    use rustix::{
        io::write,
        process::{Signal, getpid, kill_process},
    };
    use tokio::time::timeout;

    use super::*;

    /// **The channel flips** — the wiring leg, and the one the process-level legs are built on: the
    /// confined `SIGTERM` scenario in `crates/tinio-e2e`'s `confinement.feature` drives this same
    /// channel through the real binary under a seccomp filter, and the Windows console-handler case
    /// lives in `crates/tinio/tests/lifecycle.rs`, where `CARGO_BIN_EXE_tinio` resolves, because no
    /// scenario can raise a console control event. The test's own process is the only place a
    /// handler can be installed, and registration is process-wide and permanent: **this is the
    /// module's only test that may raise a termination signal**, because the fast-exit arm counts a
    /// second one in *any* drain and would call `process::exit(0)`, truncating the run green.
    #[tokio::test]
    async fn a_signal_flips_the_shutdown_channel() {
        let handles = install().expect("install");
        let mut shutdown = handles.shutdown.clone();
        kill_process(getpid(), Signal::TERM).expect("raise");
        timeout(Duration::from_secs(5), shutdown.changed())
            .await
            .expect("the channel flips")
            .expect("sender alive");
        assert!(*shutdown.borrow());
    }

    /// **The wake answers a full socket instead of waiting** — the wedge's prevention, asserted on
    /// the socket itself rather than on a race. A burst fills the write end (measured: 278 one-byte
    /// sends before `EAGAIN`), and a handler that *waits* for space there can block the only thread
    /// that would make space, so this flag is what keeps a burst from wedging the process.
    ///
    /// **The flood-shaped version of this leg was measured not to discriminate**, and the honest
    /// record of that is here rather than an assertion that cannot fail: senders inside the target
    /// process are themselves signal takers, the drain keeps up with them, the socket never fills,
    /// and the blocking draft of this file passed a `SIGHUP` burst followed by `SIGTERM`. What
    /// reproduces the wedge is a sender *outside* the process (the review's flood: one `kill -HUP`
    /// loop against a confined server left all 42 threads in `sock_alloc_send_pskb` with `SIGTERM`
    /// ignored), which is a product-level shape and lives in that evidence, not in a unit test.
    ///
    /// **Hermetic beside the wiring leg**: handlers registered through the product's own path, but
    /// no drain and no signal raised — so this test neither counts a termination nor can reach
    /// `process::exit`, and a handler that runs on its pair later (the `SIGTERM` the wiring leg
    /// raises) finds a full socket and must answer `EAGAIN`, which is the point.
    #[test]
    fn the_wake_answers_a_full_socket_instead_of_waiting() {
        let (mut reader, writer) = UnixStream::pair().expect("socketpair");
        let writer = Arc::new(writer);
        register_wakes(&writer).expect("register");

        // Fill the socket the handlers write to, **through that same writer**. The loop is on a
        // thread of its own because the failure this asserts *is* a write that waits — the thread
        // is where the waiting would happen, and the bound below is how the test observes it
        // instead of hanging on it.
        let (sent, filled) = mpsc::channel();
        let filler = Arc::clone(&writer);
        thread::spawn(move || {
            let mut wrote = 0usize;
            while write(&*filler, b"F").is_ok() {
                wrote += 1;
            }
            let _ = sent.send(wrote);
        });
        let wrote = filled.recv_timeout(Duration::from_secs(2)).expect(
            "the write end the handlers hold must be non-blocking: a full socket has to answer \
             `EAGAIN`, and a blocking one leaves this thread waiting inside the write — the wedge",
        );
        assert!(wrote > 0, "the fill must have written at least one byte");

        // And the bytes really are in that socket, so the fill was no no-op on a closed pair.
        let mut sink = [0u8; 4096];
        let mut got = 0usize;
        while got < wrote {
            match reader.read(&mut sink) {
                Ok(0) => break,
                Ok(read) => got += read,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        assert!(
            got >= wrote,
            "the socket held {got} of the {wrote} bytes the fill wrote"
        );
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
