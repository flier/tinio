//! The SCM dispatcher. A process the Service Control Manager started is a service,
//! not a command line; `try_run_as_service` answers before any parser sees argv.

use std::{
    ffi::OsString,
    iter,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use either::Either;
use tokio::sync::watch;
use windows_service::{
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
    service_dispatcher,
};

use super::SERVICE_NAME;
use crate::{
    Error,
    cli::Command,
    commands::start::{self, Server},
    run::{Resolved, command_line, on_runtime},
};

// ---------------------------------------------------------------------------------------
// The SCM's half: the one place in this crate where a server is started by the operating
// system rather than by a command line.
// ---------------------------------------------------------------------------------------

/// The SCM's own window for a pending start or stop before it assumes the service hung: the
/// drain bound `contracts/cli.md` names for `POST /stop`, which is exactly what the stop
/// control below has to finish inside.
const PENDING_HINT: Duration = Duration::from_secs(10);

/// The stop the SCM asks for, and the channel it has to reach (decision 12: one channel, and
/// the SCM's control is the fourth entry point onto it, beside the terminations signals,
/// `POST /stop` and the data plane's own drain).
///
/// The two halves move independently: the control handler runs on a thread the SCM creates
/// and can be told to stop **before** `start::run` has built the process's channel, and the
/// channel is handed over the moment that half returns. One mutex makes the two orderings
/// equivalent — a request that arrives first is sent when the channel appears, and a channel
/// that appears first takes the pending request with it — so the window between
/// `StartPending` and the serving half is a **delayed** stop rather than a lost one, which is
/// the difference between a service that drains and one the SCM has to kill.
///
/// The sections it guards are a field write, a sender clone and a non-blocking `watch` send:
/// the system thread that runs the handler is never parked behind anything slow.
struct StopRequest(Mutex<StopState>);

/// What [`StopRequest`] guards: the process's channel, once it exists, and whether a stop is
/// waiting for it.
#[derive(Default)]
struct StopState {
    /// The one shutdown channel — the sender `Plane::bind` also hands to `POST /stop`.
    stopper: Option<watch::Sender<bool>>,
    /// A stop that arrived before the channel did.
    requested: bool,
}

impl StopRequest {
    /// Nothing asked for yet, no channel yet.
    fn new() -> Self {
        Self(Mutex::new(StopState::default()))
    }

    /// The SCM's stop control: send on the channel, or keep the request for the one that is
    /// coming.
    fn request(&self) {
        let mut state = self.lock();
        state.requested = true;
        if let Some(stopper) = state.stopper.clone() {
            let _ = stopper.send(true);
        }
    }

    /// The serving half's channel, handed over as soon as `start::run` has built it. Answers
    /// whether the SCM had already asked for a stop, so its caller knows not to report
    /// `Running` for a service that is already draining.
    fn publish(&self, stopper: watch::Sender<bool>) -> bool {
        let mut state = self.lock();
        let requested = state.requested;
        state.requested = false;
        state.stopper = Some(stopper.clone());
        if requested {
            let _ = stopper.send(true);
        }
        requested
    }

    /// The guard, with its poison recovered: nothing inside these sections can panic, so a
    /// poisoned lock is some other thread's failure — and refusing to stop would be a worse
    /// answer to it than stopping.
    fn lock(&self) -> MutexGuard<'_, StopState> {
        self.0.lock().unwrap_or_else(|err| err.into_inner())
    }
}

/// Whether this process was started by the SCM — and if so, the service's whole life.
///
/// The dispatcher answers `Err` **immediately** when the process was not started by the SCM
/// (`StartServiceCtrlDispatcherW` fails with `ERROR_FAILED_SERVICE_CONTROLLER_CONNECT`), which
/// is what lets `run` put this before the CLI parse: an ordinary `tinio doctor` pays nothing
/// for it. A `true` answer means the service has already run to completion, because
/// `service_dispatcher::start` blocks until the SCM reports the service stopped.
pub(crate) fn try_run_as_service() -> bool {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main).is_ok()
}

// The FFI entry point `service_dispatcher::start` registers, on the thread the SCM creates:
// `define_windows_service!` builds the `extern "system" fn(u32, *mut *mut u16)` the
// dispatcher's signature takes, parses the SCM's wide-character argument vector and calls
// `service_main` with it. The macro is the crate's, so the one `unsafe` inside it (that raw
// vector) stays in `windows-service` rather than becoming an `#[allow(unsafe_code)]` site of
// ours — and the comment is not a doc comment because rustdoc generates no documentation for
// a macro invocation, which is a `-D warnings` failure.
windows_service::define_windows_service!(ffi_service_main, service_main);

/// The service, on the thread the SCM creates for it: the control handler, the status
/// reports, and then the server the service's registered `binPath` names.
fn service_main(arguments: Vec<OsString>) {
    // The handler needs the status handle and the handle is what registering answers, so the
    // one is published to the other, on an `Arc` the `'static` closure can own, before any
    // control can arrive.
    let published = Arc::new(OnceLock::new());
    let request = Arc::new(StopRequest::new());
    let handle = match service_control_handler::register(SERVICE_NAME, {
        let published = Arc::clone(&published);
        let request = Arc::clone(&request);
        move |event| control_event(&published, &request, event)
    }) {
        Ok(handle) => handle,
        Err(err) => {
            tracing::error!(error = %err, "the SCM control handler could not be registered");
            return;
        }
    };
    let _ = published.set(handle);
    if let Err(err) = set_status(&handle, ServiceState::StartPending, NO_ERROR, PENDING_HINT) {
        tracing::error!(error = %err, "the pending start could not be reported to the SCM");
        return;
    }

    let exit_code = match run_service(arguments, &handle, &request) {
        Ok(()) => NO_ERROR,
        Err(err) => {
            tracing::error!(error = %err, "the service stopped with an error");
            ServiceExitCode::ServiceSpecific(u32::from(err.exit_code()))
        }
    };
    if let Err(err) = set_status(&handle, ServiceState::Stopped, exit_code, Duration::ZERO) {
        tracing::error!(error = %err, "the final status could not be reported to the SCM");
    }
}

/// The SCM's controls, answered on a thread of its own.
fn control_event(
    published: &OnceLock<ServiceStatusHandle>,
    request: &StopRequest,
    event: ServiceControl,
) -> ServiceControlHandlerResult {
    match event {
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        ServiceControl::Stop | ServiceControl::Shutdown | ServiceControl::Preshutdown => {
            // The stop goes onto the process's **one** shutdown channel — the same one a
            // termination signal and `POST /stop` send on (decision 12) — so a supervised stop
            // drains exactly like the rest: cease accepting, drain ≤ 10 s, remove the socket
            // and `state`, exit 0. `Ready::handles()` is the seam that reaches it
            // (`commands/start.rs`), and `StopRequest` is what carries the request across the
            // window in which that channel does not exist yet.
            //
            // The status says what is happening while it happens: `StopPending` for the drain,
            // and `Stopped` with the run's exit code from `service_main` once `serve` returns.
            // The hint is the drain bound, so the SCM waits inside it rather than assuming a
            // hung service — and if the drain ever exceeds it, terminating the process is the
            // SCM's answer to a service that overran its own bound.
            if let Some(handle) = published.get() {
                let _ = set_status(handle, ServiceState::StopPending, NO_ERROR, PENDING_HINT);
            }
            request.request();
            ServiceControlHandlerResult::NoError
        }
        _ => ServiceControlHandlerResult::NotImplemented,
    }
}

/// The refusal an SCM start gets when its registered argv is not `tinio server <DIR>` — one
/// author, for the two shapes that can arrive here (another command, or no command at all).
fn not_a_server() -> Error {
    Error::Usage(format!(
        "`{SERVICE_NAME}` was started with an argument vector that is not a server command: the \
         service's registered binPath must be `tinio server <DIR>`"
    ))
}

/// The server, as a service: the SCM's argument vector through the same [`command_line`] both
/// entry points run, then Task 3's pre-runtime half and the process's one runtime.
///
/// The argv source is the dispatcher's whole consequence: [`try_run_as_service`] runs *before*
/// the CLI parse, so its caller has no command line to inherit — one sequence, one argv shape,
/// and the only decision left to the SCM's path is who supplies the vector. `command_line`
/// supplies the rest, `.env` included: a service-started server must resolve exactly as one
/// started from a shell does.
fn run_service(
    arguments: Vec<OsString>,
    handle: &ServiceStatusHandle,
    request: &StopRequest,
) -> Result<(), Error> {
    let argv: Vec<OsString> = iter::once(OsString::from("tinio"))
        .chain(arguments)
        .collect();
    let Either::Left(resolved) = command_line(&argv)? else {
        return Err(not_a_server());
    };
    let Resolved {
        cli,
        provenance,
        located,
    } = *resolved;
    let Command::Server(args) = cli.command else {
        return Err(not_a_server());
    };

    match start::run(args, located, None, &provenance)? {
        Server::Ready(mut ready) => {
            // Stop channel first so a stop that arrived during start takes effect; `Running`
            // only after `serve` publishes `state` (see `Ready::set_on_ready`).
            if !request.publish(ready.handles().stopper.clone()) {
                let handle = *handle;
                ready.set_on_ready(move || {
                    let _ = set_status(&handle, ServiceState::Running, NO_ERROR, Duration::ZERO);
                });
            }
            on_runtime(start::serve(*ready)).map(|_| ())
        }
        Server::Launched(_) => {
            // The macOS/Windows launcher arm: a sandboxed run forked, confined and supervised
            // its child to completion before `start::run` returned, so there is no serving half
            // left to report `Running` for. The child's status is not readable back out of
            // `ExitCode` — `std` exposes no accessor — so the SCM is told the run finished.
            //
            // **The stop consequence of this arm is stated in the contract rather than handled
            // here.** A launcher never reaches `publish`, so a stop arriving while the child
            // runs stays in `StopRequest` and the SCM ends this process at the wait hint: with
            // confinement enabled, `stop` is not the graceful path `contracts/cli.md` promises
            // for every other configuration, and that qualifier is where it is written down.
            // Carrying a stop *into* the launcher (so the child drains too) is the sandbox
            // plans' work — their shared §4 serving signature is what takes a `stop` channel —
            // and the configuration is unreachable until a platform backend exists at all:
            // `sandbox::plan` refuses every confinement request today, with the two refusals
            // `contracts/cli.md`'s `--sandbox` bullet specifies (a usage error naming the build
            // when the `sandbox` feature is off, a startup error naming the platform while the
            // backend is a stub), and `sandbox::BACKEND_IS_STUB` is `true` on all three targets,
            // so `confine` is never reached and this arm is not taken.
            tracing::debug!("the confined child has already exited");
            Ok(())
        }
    }
}

/// The status the SCM is told: the service type, the controls it accepts, and the state.
///
/// `SHUTDOWN` and `PRESHUTDOWN` are mutually exclusive (MSDN): a service that takes the
/// machine's shutdown notification registers the former, and [`control_event`] answers the
/// latter whether or not it is accepted.
fn set_status(
    handle: &ServiceStatusHandle,
    state: ServiceState,
    exit_code: ServiceExitCode,
    wait_hint: Duration,
) -> Result<(), Error> {
    handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            exit_code,
            // A pending state needs a non-zero checkpoint and every other state must carry
            // zero (MSDN): one expression rather than a value each caller has to remember.
            checkpoint: u32::from(matches!(
                state,
                ServiceState::StartPending | ServiceState::StopPending
            )),
            wait_hint,
            process_id: None,
        })
        .map_err(|err| {
            Error::Operational(format!(
                "the service status could not be reported to the SCM: {err}"
            ))
        })
}

/// A run that succeeded, named so the two reporting sites above read the same way.
const NO_ERROR: ServiceExitCode = ServiceExitCode::NO_ERROR;

#[cfg(test)]
mod tests {
    use super::*;

    // The Windows half's unit, and the only leg it has on this side: the SCM itself cannot be
    // driven from a test process (it is the OS that starts services, and only for services it
    // has registered), so what is asserted is the ordering the handshake turns on — a stop that
    // arrives before the process's channel exists, and one that arrives after it.
    #[test]
    fn a_stop_is_never_lost_to_the_start_window() {
        use tokio::sync::watch;

        // The window the SCM's short start budget makes real: a stop that lands between
        // `StartPending` and the serving half's channel. The request is kept, and handing the
        // channel over sends it — a delayed stop, never a dropped one.
        let request = StopRequest::new();
        request.request();
        let (stopper, mut shutdown) = watch::channel(false);
        assert!(request.publish(stopper), "the request was already there");
        assert!(*shutdown.borrow_and_update(), "and it reached the channel");
    }

    #[test]
    fn a_stop_after_the_channel_exists_is_sent_at_once() {
        use tokio::sync::watch;

        let request = StopRequest::new();
        let (stopper, mut shutdown) = watch::channel(false);
        assert!(!request.publish(stopper), "nothing had been asked for yet");
        assert!(!*shutdown.borrow_and_update(), "and nothing was sent");
        request.request();
        assert!(
            *shutdown.borrow_and_update(),
            "the SCM's stop reaches the channel the signals use"
        );
    }
}
