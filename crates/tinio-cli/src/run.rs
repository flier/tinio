//! The process entry point and design §1's sequence.
//!
//! [`run()`] is the public entry; [`command_line`] is the two-pass parse both entry points
//! share; [`on_runtime`] is the process's one runtime, built last.

use std::{env, ffi::OsString, future::Future, iter, process::ExitCode};

use either::{Either, Left, Right};
use tokio::runtime::Builder;

#[cfg(feature = "api")]
use crate::commands::{status, stop};
use crate::{
    Error,
    cli::{self, Cli, Command, Provenance},
    commands::{doctor, start, start::Server},
    events,
    resolve::{self, Located},
    sandbox, service,
};

/// Entry point for the `tinio` CLI.
///
/// Maps every outcome onto the documented exit codes: `0` success, `1` operational error, `2`
/// usage error.
///
/// # Examples
///
/// ```no_run
/// let code = tinio_cli::run();
/// ```
pub fn run() -> ExitCode {
    // The SCM's dispatcher, before any parser sees argv (decision 22): a process the SCM
    // started is a service rather than a command line, and the dispatcher answers `Err`
    // immediately for every other process — so an ordinary `tinio doctor` pays nothing for
    // this, and it is why the arm can sit ahead of the parse at all.
    #[cfg(windows)]
    if service::try_run_as_service() {
        // The service has already run to completion, and the SCM read the outcome off the
        // status report `service_main` sent it rather than off this process's exit code.
        return ExitCode::SUCCESS;
    }
    match try_run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("tinio: {err}");
            ExitCode::from(err.exit_code())
        }
    }
}

/// §1's sequence, as it is before the runtime exists.
fn try_run() -> Result<ExitCode, Error> {
    // Step 1 of the sequence: a confined child is identified before any parser sees argv,
    // because `tinio-sandbox-fd=<n>` is not a flag clap would accept.
    let (argv, adopted) = sandbox::filter_argv(env::args_os().skip(1).collect())?;
    let argv: Vec<OsString> = iter::once(OsString::from("tinio")).chain(argv).collect();

    // Steps a–e of §1, shared with the SCM's entry point: the discovery parse, `.env`, and the
    // authoritative parse — see [`command_line`] for the order and for what each pass answers.
    // The two arms are the two answers it can give, and the dispatch below runs on the
    // **authoritative** parse: every value and `ValueSource` from here on is that one's.
    // `adopted`'s text travels with it: a confined child's discovery must not re-validate the
    // explicit `--config`, whose file may lie outside every exposure (see [`command_line`]).
    let parsed = command_line(&argv, adopted.as_ref().map(|adopted| adopted.config()))?;

    match parsed {
        // `service`, answered by the discovery parse alone (`command_line`'s doc states why).
        Either::Right(args) => service::run(args),
        Either::Left(resolved) => {
            let Resolved {
                cli,
                provenance,
                located,
            } = *resolved;
            match cli.command {
                // `start::run` is §1's pre-runtime half — resolve, fork, confine — so it
                // returns before a runtime exists, and the arm builds the process's one for
                // the half it leaves (decision 2). A launcher returns its child's status and
                // never reaches it.
                // Decision 19's other half, and the reason the frame sits here rather than in
                // `run`'s error arm: **only the serving path owes stdout a frame.** A refusal
                // before the readiness point is an `Error` frame beside the `tinio: …` line
                // stderr carries, so a harness waiting for a frame learns why nothing started
                // instead of timing out; `status`, `stop` and `doctor` keep their own stdout,
                // where a report is not a frame. A failure *after* the readiness frame is not
                // framed — that is the serving phase, not a startup refusal.
                Command::Server(args) => match start::run(args, located, adopted, &provenance) {
                    Ok(Server::Launched(code)) => Ok(code),
                    Ok(Server::Ready(ready)) => on_runtime(start::serve(*ready)),
                    Err(err) => {
                        events::emit_error(&err.to_string());
                        Err(err)
                    }
                },
                #[cfg(feature = "api")]
                Command::Status(args) => on_runtime(status::run(args, located)),
                #[cfg(feature = "api")]
                Command::Stop(args) => on_runtime(stop::run(args, located)),
                // Not feature-gated, and `cli::Command`'s doc is why: `doctor` diagnoses the
                // always-present layers, so only the parts of it that need the plane are.
                Command::Doctor(args) => on_runtime(doctor::run(args, located)),
                // Unreachable — `command_line` answers `service` with the arm above — and
                // spelled out rather than `unreachable!()`, because library code does not
                // panic. The answer is the helper's own, so a future edit cannot dispatch it
                // wrongly.
                Command::Service(args) => service::run(args),
            }
        }
    }
}

/// §1 steps a–e, as **both** entry points run them (design decision 3): the discovery parse,
/// `<state dir>/.env`, and the authoritative parse.
///
/// The same argv is parsed twice because clap reads the environment at parse time and the file
/// is not in that environment until the state dir is known. The first pass — the **discovery**
/// one — answers what [`Command::discovery`] lists and nothing else: `DIR`, `--config`, the
/// mode's two channels and `TINIO_STATE_DIR`, which are exactly the values `.env`'s own
/// location is a function of (decision 4: a file whose location is a function of the mode
/// cannot also select it). dotenvy then writes the file's values into the process environment
/// without overriding an exported one, which is what keeps the process environment above the
/// file, and the second pass is the authoritative one: every value and [`Provenance`] the
/// resolution reads is that parse's.
///
/// **`service` is answered here, and by the discovery pass alone** — it is the one command that
/// reaches no resolution: it reads no configuration file (the unit it installs is what will
/// read one), so `.env` is not part of its chain and no value from that file may decide a
/// unit's argv. Its two channels are the invoking environment's, which is also what makes the
/// argv a unit records the argv the server resolves in the same environment.
///
/// **`adopted` is the confined child's own fact**, and it changes one thing in the discovery
/// resolution: a child's `--config` names the file its launcher read and forwarded as text, and
/// that path may sit outside every exposure — so the child resolves the mode from the text
/// instead of re-validating a path it may not be able to see (`resolve::locate_confined`'s doc
/// states the ruling). `None` is every other process, which keeps the explicit-path contract
/// exactly as it was: `--config` must name a file that is there, and a mistyped one is a usage
/// error.
///
/// # Errors
///
/// A usage error from either parse (exit 2) and whatever [`locate`](crate::resolve::locate) or
/// the `.env` load refuses.
pub(crate) fn command_line(
    argv: &[OsString],
    adopted: Option<&str>,
) -> Result<Either<Box<Resolved>, service::Args>, Error> {
    let (discovery, matches) = cli::parse(argv)?;
    let provenance = cli::provenance(&matches);
    match discovery.command {
        Command::Service(args) => Ok(Right(args)),
        command => {
            let located = match adopted {
                Some(text) => resolve::locate_confined(command.discovery(), &provenance, text)?,
                None => resolve::locate(command.discovery(), &provenance)?,
            };
            resolve::apply_env_file(&located.state_dir)?;
            let (cli, matches) = cli::parse(argv)?;
            let provenance = cli::provenance(&matches);
            Ok(Left(Box::new(Resolved {
                cli,
                provenance,
                located,
            })))
        }
    }
}

/// The three answers a resolving command needs: the **authoritative** parse, the provenance
/// that says which channel answered each key, and the discovery answers the resolution is
/// built on.
///
/// Carried as [`Either::Left`] from [`command_line`] (boxed: a whole `Cli` is large enough that
/// `clippy::large_enum_variant` would fire on an unboxed side). `Either::Right` is `service`.
pub(crate) struct Resolved {
    /// The second parse's command line — the values every command after this reads.
    pub(crate) cli: Cli,
    /// Which channel answered each env-backed key, off that parse's matches.
    pub(crate) provenance: Provenance,
    /// The root, the mode and the state dir, resolved before `.env` was loaded.
    pub(crate) located: Located,
}

/// The process's **one** runtime — the only construction site in the crate.
///
/// Every command that needs a runtime is dispatched through this function, and none of them —
/// `server`, `status`, `stop` or `doctor` — builds one of its own: `status` and `stop` are
/// network clients whose probe and poll run on this runtime, and a
/// `Builder::new_current_thread()` beside it would be exactly the second runtime decision 2
/// exists to forbid. It is built **here**, at the end of the sequence, because §1 step k must
/// follow the fork and the confinement: creating a runtime and then forking is the hazard the
/// order avoids.
pub(crate) fn on_runtime<F>(future: F) -> Result<ExitCode, Error>
where
    F: Future<Output = Result<ExitCode, Error>>,
{
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::Operational(format!("the runtime could not start: {err}")))?
        .block_on(future)
}
