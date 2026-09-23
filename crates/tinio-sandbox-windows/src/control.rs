//! The signal relay: forward a console stop to the confined child (design §3
//! step 9). Windows has no `SIGTERM`. `CTRL_BREAK_EVENT` is the event that can
//! be addressed to a process group, and the child is created with
//! `CREATE_NEW_PROCESS_GROUP` ([`crate::process`]).
//!
//! The handler only forwards (`GenerateConsoleCtrlEvent`) or signals an event
//! object. It does not allocate, lock, or emit `tracing` — a console control
//! thread may not. Arming, delivery, and [`on_stop`] log from threads that may.
//! This module is an enumerated `unsafe` site (decision 14).

#![allow(unsafe_code)]

use std::{
    ptr::null,
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    thread,
};

use windows_sys::Win32::{
    Foundation::{HANDLE, WAIT_OBJECT_0},
    System::{
        Console::{
            CTRL_BREAK_EVENT, CTRL_C_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT,
            CTRL_SHUTDOWN_EVENT, GenerateConsoleCtrlEvent, SetConsoleCtrlHandler,
        },
        Threading::{CreateEventW, INFINITE, SetEvent, WaitForSingleObject},
    },
};

use crate::{
    _core::{Result, launch},
    handles::last_error,
};

/// The child's process id — the only state the handler reads.
///
/// A process id rather than a handle: `GenerateConsoleCtrlEvent` takes a process
/// *group* id, the child is its own group leader because the launch created it
/// with `CREATE_NEW_PROCESS_GROUP`, and an integer needs no ownership, no
/// closing and no lifetime reasoning on a thread the system created. Zero means
/// "no child addressed", which is also what a child-side hook leaves here.
static CHILD: AtomicU32 = AtomicU32::new(0);

/// The relay's event: a handle in the shape the thread that waits on it needs.
///
/// **The wrapper exists because `windows_sys`'s `HANDLE` is a `*mut c_void`,
/// which is neither `Send` nor `Sync`** — so it can neither be captured by the
/// thread [`on_stop`] spawns nor held in the `static` below. See the two marker
/// impls for what sharing one actually asserts.
///
/// `Debug` is derived for the one `expect` on this cell ([`relay_event`]'s), whose
/// `Result` carries the value it refused: what it prints is the handle, which is
/// an identifier of a kernel object rather than an address in this process.
#[derive(Clone, Copy, Debug)]
struct Event(HANDLE);

// SAFETY: a Win32 event handle is an identifier of a kernel object, not a
// pointer into this process's memory, and nothing here dereferences one. The
// value is created before any other thread can see it, it is never closed, and
// `SetEvent` and `WaitForSingleObject` are calls the kernel accepts from any
// thread of the process — which is exactly the sharing these two impls permit.
unsafe impl Send for Event {}
// SAFETY: as above. The cross-thread accesses this value has are the console
// handler reading it from a thread the system created, and the hook's own thread
// waiting on it.
unsafe impl Sync for Event {}

impl Event {
    /// Wait until the event is signalled, and answer with what the wait said.
    ///
    /// **A method rather than the call at its use site, and that is a capture
    /// rule rather than taste**: a closure that names `event.0` captures the raw
    /// handle *field* — Rust's closures capture disjoint fields — so it would
    /// carry a `*mut c_void`, which is not `Send` whatever the type around it
    /// asserts. Taking `self` captures the whole `Event`, which is the value the
    /// marker impls above are about.
    ///
    /// `INFINITE`, because what ends this wait is the stop itself: the hook lives
    /// for as long as the process does.
    fn wait(self) -> u32 {
        // SAFETY: `self.0` is this process's own event, created before the
        // handler was installed and never closed, and waiting on a live event
        // handle is what the handle is for.
        unsafe { WaitForSingleObject(self.0, INFINITE) }
    }
}

/// The process-wide manual-reset event the handler signals.
///
/// One event for the process: a launcher relays, a child runs its hook, and no
/// process is both. The handler must not create it — a console handler runs on
/// an arbitrary thread at an arbitrary moment — so the cell is initialised by
/// [`arm`] or [`on_stop`], both of which run on the ordinary path, and the
/// handler only reads it.
static EVENT: OnceLock<Event> = OnceLock::new();

/// Arm the relay for `child`.
///
/// Called by [`crate::backend`](mod@crate::backend) once the child exists and is owned by the job,
/// and before anything can stop the launcher: a stop that arrives before there
/// is a child has nothing to forward to, and from this point on the job's
/// kill-on-close covers the launcher dying first (design §3 step 9, decision 20).
///
/// **One armed relay per process, and arming twice is not one relay.** The
/// system accepts a second registration of the same routine rather than
/// recognising it as one (measured on this host, 2026-09-21: `add = 1` twice,
/// both calls answer 1), so a process that launches more than once — the test
/// harness, and a CLI that serves twice — [`disarm`]s before the next `arm`
/// instead of leaving two entries to run the forward twice. What is armed is a
/// console control *handler*, which lives until it is removed; this is not a
/// per-launch registration that expires.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the handler cannot be
/// installed or the relay's event object cannot be created. The id is cleared on
/// the way out, so a launch that failed here leaves the process exactly as it
/// found it rather than addressed at a child it never supervised.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::control;
///
/// // The child's process id, which is also its process-group id: the launch
/// // created it with `CREATE_NEW_PROCESS_GROUP`.
/// control::arm(1234)?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn arm(child: u32) -> Result<()> {
    // The event first, and then the id: everything the handler touches exists
    // before it can be called, and a failure at either step leaves the process
    // with nothing addressed rather than with an id no handler will read.
    relay_event()?;
    // The id second, before the handler is installed: a forward that arrives once
    // the handler is in place must address this child rather than whatever a
    // previous launch left behind.
    CHILD.store(child, Ordering::SeqCst);
    // SAFETY: `console_handler` is a plain `extern "system"` function whose
    // signature is the `PHANDLER_ROUTINE` that `SetConsoleCtrlHandler` takes, and
    // everything it touches — one atomic and one event handle created above —
    // lives for the process's lifetime.
    let ok = unsafe { SetConsoleCtrlHandler(Some(console_handler), 1) };
    if ok == 0 {
        CHILD.store(0, Ordering::SeqCst);
        return Err(launch("SetConsoleCtrlHandler", "relay", last_error()));
    }
    // Shared §7's "armed relay", with the value the forward will address: the
    // child's process id, which is its process-group id because it was created
    // with `CREATE_NEW_PROCESS_GROUP`.
    tracing::debug!(
        step = "SetConsoleCtrlHandler",
        role = "launcher",
        child = child
    );
    Ok(())
}

/// Deliver a stop to `group`'s process group.
///
/// This is what the relay forwards, and it is public because it is also how a
/// test plays the container's part: a console control event addressed at a
/// process group, which is the whole of what a Windows container does to the
/// process it started.
///
/// **`Ok` means the call was accepted, not that the event was delivered.**
/// `GenerateConsoleCtrlEvent` returns 1 with `GetLastError` 0 for a
/// group-addressed `CTRL_C_EVENT` and delivers nothing at all (measured; the
/// design's evidence table has the rows). What this function sends is
/// `CTRL_BREAK_EVENT`, which *is* delivered to the group addressed — and it is
/// the only console control event that can be limited to one. A caller that needs
/// to know the stop arrived has to watch the far end: the child's hook, not this
/// call's return value.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the call is refused —
/// there is no console to deliver on, or the group does not name one that
/// exists.
///
/// # Examples
///
/// ```no_run
/// use std::process;
///
/// use tinio_sandbox_windows::control;
///
/// // Addressed at this process's own group, which every process in it
/// // receives: for one that armed a relay, that is the forward.
/// control::send_stop(process::id())?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn send_stop(group: u32) -> Result<()> {
    // SAFETY: GenerateConsoleCtrlEvent takes two integers and dereferences
    // nothing.
    let ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, group) };
    if ok == 0 {
        return Err(launch(
            "GenerateConsoleCtrlEvent",
            group.to_string(),
            last_error(),
        ));
    }
    // Accepted, not delivered — the doc above is explicit about that, so the
    // event says what this function knows: the call went out, addressed at
    // `group`.
    tracing::debug!(
        step = "GenerateConsoleCtrlEvent",
        group = group,
        event = "CTRL_BREAK_EVENT"
    );
    Ok(())
}

/// Unhook the relay, and put the process back on the default disposition.
///
/// **Removing it takes the same routine, and that is the whole of this function
/// rather than a detail of it.** `console_handler` answers `TRUE` for every event
/// it matches, and `TRUE` means *handled*: a handler left installed would go on
/// swallowing `CTRL_C`, `CTRL_BREAK`, `CTRL_CLOSE` and the logoff and shutdown
/// events instead of relaying them, so the process would neither be stopped by
/// them nor act on them — it would ignore a Ctrl+C, and it would neither act on a
/// closing window nor on a logoff or shutdown, the events that normally end a
/// console session and the ones the system waits on, up to its own short grace
/// period, before it stops waiting. Swallowing those is the version of this that
/// is hardest to notice and hardest to recover from. A `NULL` routine with
/// `add = 0` does **not** remove a registered routine: it clears the
/// "ignore Ctrl+C" entry, which this process never set. Passing
/// `Some(console_handler)` back with `add = 0` is the documented removal, and
/// after it the process behaves like one that never armed a relay.
///
/// `serve`'s launcher arm never reaches this call — it forwards the child's
/// status and exits — so the window this closes is the one a process that
/// outlives its launch has: the test harness, and any future CLI that serves
/// twice. The id is cleared first, so a stop that arrives between the two calls
/// is swallowed rather than forwarded to a child that is already gone.
///
/// If the removal itself fails, the handler *is* still installed, and that is the
/// one case this function cannot hide: nothing is addressed any more, so it
/// forwards nothing — but it goes on swallowing, and the returned error is the
/// only notice of it.
///
/// **A process that never armed is refused rather than answered.** Removing a
/// routine that was never installed fails with `ERROR_INVALID_PARAMETER`
/// (measured on this host, 2026-09-21: `add = 0` for an unregistered routine
/// answers 0 with `GetLastError` 87, while removing one that was installed
/// answers 1), which is the right answer rather than an inconvenience: the call
/// is made by a process that has decided it is carrying a relay, and one that is
/// not is a process whose own idea of its state is wrong. Removing an installed
/// routine does restore the default disposition — the same measurement has the
/// process die of the next stop instead of surviving it, its handler having gone
/// with the registration.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the handler cannot be
/// removed: it is not installed (above), or the call was refused.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::control;
///
/// control::arm(1234)?;
/// control::disarm()?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn disarm() -> Result<()> {
    CHILD.store(0, Ordering::SeqCst);
    // SAFETY: the same `console_handler` this module installs, with `add = 0`,
    // which is the documented way to remove that routine; the pointer is passed
    // through and never dereferenced.
    let ok = unsafe { SetConsoleCtrlHandler(Some(console_handler), 0) };
    if ok == 0 {
        return Err(launch("SetConsoleCtrlHandler", "disarm", last_error()));
    }
    tracing::debug!(step = "SetConsoleCtrlHandler", role = "disarm");
    Ok(())
}

/// Run `handler` when this process is stopped.
///
/// The child-side counterpart: the confined server needs somewhere for a
/// forwarded stop to land, or it dies on the spot instead of draining. **The
/// handler runs on a thread of its own rather than in the console handler**, so
/// it may take locks, allocate, and reach a runtime — which is what makes this
/// the hook the CLI's graceful shutdown (T066/T067) lands on, and what lets the
/// stop hook write a file and end its process in a test.
///
/// One hook per process, like [`arm`]. A second call leaves two threads waiting
/// on the same manual-reset event, and one stop runs both closures.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the hook cannot be armed:
/// the handler was refused, or the event object the hook's thread waits on could
/// not be created.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::control;
///
/// // The child's own hook. It runs on a thread of its own, so what it does may
/// // allocate and take locks: signal the runtime's shutdown here.
/// control::on_stop(|| {
///     let _ = ();
/// })?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn on_stop(handler: impl Fn() + Send + 'static) -> Result<()> {
    // The event first, then the handler, then the thread — and the order is the
    // one that leaves nothing behind on a failure. A stop that arrives in the gap
    // between the two is not lost either: the event is a **manual-reset** one, so
    // the signal the handler sets is still there when the thread starts waiting.
    let event = relay_event()?;
    // SAFETY: the same `console_handler`, in the child: it forwards nothing
    // (`CHILD` is zero here) and signals the event the thread below turns into
    // the caller's closure. **It is named `console_handler` and not `handler`
    // because the parameter above already took that name** — a closure cannot be
    // handed to `SetConsoleCtrlHandler`, whose parameter is a `PHANDLER_ROUTINE`
    // (`Option<unsafe extern "system" fn(u32) -> i32>`).
    let ok = unsafe { SetConsoleCtrlHandler(Some(console_handler), 1) };
    if ok == 0 {
        return Err(launch("SetConsoleCtrlHandler", "stop hook", last_error()));
    }
    // A thread of this process's own, with a stack: the console handler may only
    // signal the event, and everything that cannot run there runs here.
    thread::spawn(move || {
        let waited = event.wait();
        if waited != WAIT_OBJECT_0 {
            // Nothing to return it to: this thread waits for as long as the
            // process lives, so the caller is long gone. A wait that failed means
            // the hook will never run, which is a degradation worth a line in the
            // log rather than a silence (shared §7).
            tracing::warn!(
                step = "WaitForSingleObject",
                wait = waited,
                "the stop hook is dead"
            );
            return;
        }
        // **This is where a forwarded stop is traced, and the only place it can
        // be**: the handler that sent it runs on a thread the system created and
        // may not allocate or lock, which rules `tracing` out there. This thread
        // is the child's own, with a stack and a runtime to come, so it is where
        // the event belongs.
        tracing::debug!(step = "on_stop", "the stop hook fired");
        handler();
    });
    tracing::debug!(step = "SetConsoleCtrlHandler", role = "child");
    Ok(())
}

/// Create the relay's event, once per process, and hand back **that** event.
///
/// Both [`arm`] and [`on_stop`] call it: one event serves either role, and the
/// handle is never closed — it lives as long as the process, which is exactly
/// what a console handler needs of it.
///
/// **What every caller receives is the cell's event, never a second one this call
/// happens to have made.** That is what keeps the handler's signal and the hook's
/// wait on the same object: a process holding two would swallow a stop into an
/// event nobody waits on. The race that would produce one — two concurrent first
/// calls — is not one this crate can run (the confinement is entered before any
/// thread but the caller's exists, decision 26), so what the expectation below
/// states is the property rather than a window anything can reach.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the event object cannot be
/// created.
fn relay_event() -> Result<Event> {
    if let Some(event) = EVENT.get() {
        return Ok(*event);
    }
    // SAFETY: a null security descriptor and a null name are both valid for
    // `CreateEventW`, and the three remaining arguments are integers: a
    // manual-reset event in the non-signalled state.
    let handle = unsafe { CreateEventW(null(), 1, 0, null()) };
    if handle.is_null() {
        return Err(launch("CreateEventW", "relay", last_error()));
    }
    let event = Event(handle);
    // **`set`'s failure is unreachable, and it is discarded rather than answered.**
    // The two checks above are the only way into this call — the cell was read as
    // empty a few lines up and a `OnceLock` never empties — so the branch `set`
    // reports for a filled cell is the two concurrent first calls this crate
    // cannot run (the confinement is entered before any thread but the caller's
    // exists, decision 26). Three shapes were available and two are wrong: an
    // `Error` would hand a caller a failure no path can produce, and a panic —
    // `expect`, or a `debug_assert!` that unwinds in a debug build — is what this
    // crate's rules forbid in library code and what a handler on a system thread
    // must never do. What is left is to say plainly that the answer is discarded
    // because it cannot arrive, and to hand back the event this call created,
    // which is the one the cell now holds whichever caller won.
    let _ = EVENT.set(event);
    Ok(event)
}

/// The console control handler: one forward, one signal, and nothing else.
///
/// It runs on a thread the system creates, on an arbitrary stack, and it may be
/// the last thing this process does — so it reads one atomic, makes one
/// `GenerateConsoleCtrlEvent` call and signals one event. Everything that needs
/// a lock, an allocation or a runtime has already been handed to the thread that
/// waits on that event.
///
/// **It answers `TRUE` for every event it matches, and `TRUE` means *handled*
/// rather than *forwarded*.** Nothing else gets a say in that event: the
/// process's default disposition does not run, so this process is not stopped by
/// a Ctrl+C and does not end with its console window. That is what the relay
/// wants while a child is being supervised and what the child wants while a
/// forwarded stop is a request to drain rather than to die — and it is why
/// [`disarm`] removes this routine rather than leaving it installed, the
/// swallowing being meant to last exactly as long as the relay does.
///
/// **It emits no `tracing` event either, and that is a constraint rather than an
/// oversight**: every macro in that crate formats, allocates and can take a lock
/// to reach its subscriber, none of which this thread may do. The forward is
/// traced on the child's hook thread instead ([`on_stop`]), which is the process
/// on the other end of it, and the launcher's half — the arming and the id it
/// addresses — was traced by [`arm`] before any event could arrive.
///
/// **The forward is `CTRL_BREAK_EVENT` whatever arrived.** A launcher may be
/// stopped by any of the five events — a keypress, a closing window, a logoff, a
/// shutdown — and only the break event can be limited to one process group, so
/// the child is told to drain for all of them and decides the rest itself.
unsafe extern "system" fn console_handler(event: u32) -> i32 {
    match event {
        CTRL_C_EVENT | CTRL_BREAK_EVENT | CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT
        | CTRL_SHUTDOWN_EVENT => {
            let child = CHILD.load(Ordering::SeqCst);
            if child != 0 {
                // A launcher: forward to the child's group. This call is one a
                // console handler is documented to be able to make, and this is
                // its only use here. Its return value is deliberately ignored:
                // it is not evidence of delivery (see `send_stop`), and there is
                // nothing on this thread that could report it.
                // SAFETY: two integers; nothing is dereferenced.
                unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child) };
            }
            if let Some(event) = EVENT.get() {
                // A child: wake the hook's thread and let it do the real work.
                // SAFETY: the handle is this process's own event, created before
                // the handler was installed.
                unsafe { SetEvent(event.0) };
            }
            1
        }
        _ => 0,
    }
}
