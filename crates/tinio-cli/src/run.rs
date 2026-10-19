//! The process entry point and design §1's sequence.
//!
//! [`run()`] is the public entry; [`command_line`] is the two-pass parse both entry points
//! share; [`on_runtime`] is the process's one runtime, built last.

use std::{env, ffi::OsString, future::Future, iter, process::ExitCode};

use either::{Either, Left, Right};
use tokio::runtime::Builder;

#[cfg(feature = "lifecycle")]
use crate::commands::lifecycle;
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

    // **The child's own confinement, applied before anything else happens** (design
    // §10 step 1, decision 26). A launcher that resolved an in-process mechanism
    // hands the profile over on the payload, and this is the process that can apply
    // it — the apply is one-shot and irreversible, so the launcher's own process is
    // exactly the one that *cannot*. Nothing has run here yet: no `.env`, no
    // configuration file, no storage. **And it fails closed**: a child that cannot
    // apply the confinement its launcher reported is a refusal, never a server.
    //
    // **The call sits ahead of the dispatch because the mechanism is not the
    // command's**: it confines whatever this process was launched to do, and it acts
    // only on what `filter_argv`'s decode already carried. `adopted` is `None` for
    // every ordinary run, so those runs pay one `is_some` check and nothing else.
    //
    // **A free function rather than a method on `Adopted`**, which is the core's: the
    // rule is the contract's (`tinio_sandbox::apply_self_confinement`, which applies the
    // platform's own answer where one exists), and the core is `forbid` for `unsafe` and
    // knows no mechanism.
    //
    // **The call is on every target, and that is the rule rather than an omission.** Which
    // platform can apply a frame's confinement is the facade's answer, not this file's: a
    // process whose platform applies its confinement at creation or in place has no spec to
    // apply and refuses one instead of serving without it, so a `#[cfg]` here would be a
    // target where a forged frame serves unconfined. The frame's own presence is the
    // discriminator, which is the same question `adopted.is_some()` asks above.
    //
    // **The refusal is framed on stdout beside the `tinio: …` line stderr carries**,
    // for the reason every other pre-dispatch refusal is (decision 19's "a startup
    // failure is an event rather than an absence"): what fails here is a confined
    // child, which is a process a launcher is waiting on a frame from. A bare return
    // would leave that launcher decoding a child that exited without saying why.
    if let Some(adopted) = adopted.as_ref()
        && let Err(err) = sandbox::apply_self_confinement(adopted)
    {
        events::emit_error(&err.to_string());
        return Err(err);
    }

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
                #[cfg(feature = "lifecycle")]
                Command::Lifecycle(args) => on_runtime(lifecycle::run(args, located)),
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
/// **One shape reverses those two steps, and it is the pg run's** (ruling R8's sibling
/// finding): a pg run's `<state dir>/.env` is loaded *before*
/// [`locate`](resolve::locate) derives its root, when the discovery answers can already name
/// that directory (`resolve::pg::env_file_state_dir`) — the root's derivation needs the dsn,
/// and a dsn the file supplies must reach it or the root and the store would resolve two
/// different databases. The refreshed `TINIO_PG_DSN` travels on the [`Discovery`] the
/// resolution consumes, and only that field is refreshed: `DIR`, `--config`, the mode and
/// `TINIO_STATE_DIR` stay the discovery parse's, which is what keeps decision 4's rule (the
/// file cannot select the mode) true for the early load too.
///
/// **The dsn the store opens is the dsn the root was derived from, and the second resolution
/// below is what makes that hold in the other direction**: a `<state dir>/.env` that overrides
/// `[storage.pg] dsn` is read *after* the first root derivation (its own path is a function of
/// that root), so the pg arm is re-run on the authoritative answer whenever the file moved the
/// dsn. Bounded at one re-derivation, and only when the effective dsn actually changed.
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
    // **The discovery parse's provenance is the one [`locate`](resolve::locate) gets**, whatever
    // happens below: the mode is resolved from those two channels and the config file alone
    // (decision 4), so a `TINIO_READ_ONLY` line an early `.env` load puts in the environment
    // cannot start selecting the mode. The *returned* provenance is the authoritative parse's.
    let discovery_provenance = cli::provenance(&matches);
    match discovery.command {
        Command::Service(args) => Ok(Right(args)),
        command => {
            let mut discovery = command.discovery();
            // **A pg run's `<state dir>/.env` is loaded before its root is derived**, when the
            // discovery answers can already name that directory (ruling R8's sibling finding):
            // `TINIO_STATE_DIR` names a pg run's root verbatim, so `<that dir>/.tinio` does not
            // depend on the dsn — and a `TINIO_PG_DSN` the file supplies has to reach
            // `pg::resolve_dsn`, or the root would come from one dsn and the store from another,
            // and an env-only run would die naming a variable the operator did set. The value is
            // read back through the parser that owns every `TINIO_*` channel (clap), never from
            // the environment beside it.
            let early = resolve::pg::env_file_state_dir(&discovery)
                .filter(|state_dir| state_dir.join(ENV_FILE).is_file());
            if let Some(state_dir) = early.as_deref() {
                resolve::apply_env_file(state_dir)?;
                discovery.pg_dsn = cli::parse(argv)?.0.command.discovery().pg_dsn;
            }
            // The dsn this resolution derives the root from, kept so the re-derivation below can
            // tell whether the authoritative parse moved it: the section is the same document in
            // both parses, so the two answers differ exactly when the environment answered
            // differently.
            let located_dsn = discovery.pg_dsn.clone();
            let mut located = match adopted {
                Some(text) => {
                    resolve::locate_confined(discovery.clone(), &discovery_provenance, text)?
                }
                None => resolve::locate(discovery.clone(), &discovery_provenance)?,
            };
            // §1's step e, for every shape the early load could not name: the located directory's
            // own file is the one this run reads.
            if early.as_deref() != Some(located.state_dir.as_path()) {
                resolve::apply_env_file(&located.state_dir)?;
            }
            let (cli, matches) = cli::parse(argv)?;
            // **One resolved dsn feeds both.** When the file just loaded moved the dsn away from
            // the one the root was derived from (`TINIO_PG_DSN` overriding `[storage.pg] dsn`),
            // the root is derived again from the effective dsn — and the store opens that one,
            // because it is the answer of this very parse. The moved directory's own `.env` is
            // deliberately not read: the file a pg run reads is the one its **configured** root
            // holds, which is where the operator put it.
            let authoritative = cli.command.discovery().pg_dsn;
            if located.pg_root.is_some() && authoritative != located_dsn {
                discovery.pg_dsn = authoritative;
                located = match adopted {
                    Some(text) => resolve::locate_confined(discovery, &discovery_provenance, text)?,
                    None => resolve::locate(discovery, &discovery_provenance)?,
                };
            }
            let provenance = cli::provenance(&matches);
            Ok(Left(Box::new(Resolved {
                cli,
                provenance,
                located,
            })))
        }
    }
}

/// The environment file's name inside a state directory (`sources::load_env_file`'s), spelled
/// once here for the early load's existence probe — the load itself is that function's, never a
/// second reader of the file's format.
const ENV_FILE: &str = ".env";

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
