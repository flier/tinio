//! `tinio doctor` — offline diagnostics of a directory, with no server required.
//!
//! The check list is the `checks` submodule's; this module is the command body around it — the
//! resolution, the candidate probe, and the plain / `--dry-run` / `--fix` arms. The module
//! compiles in **both** builds — `cli::Command`'s doc states why — and only the items that need
//! the plane are `#[cfg(feature = "api")]`, because without a control channel the command cannot
//! prove a server is stopped and must not offer a repair it cannot guard; `--fix` and
//! `--dry-run` are gated with it, because neither can do anything in that build.

// The path is the api candidate probe's (the two-argument `state_dir_for_run`); the no-api graph
// names neither.
#[cfg(feature = "api")]
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args as ClapArgs;
use futures::StreamExt;

use super::DirArgs;
#[cfg(feature = "api")]
use crate::_api::State;
#[cfg(all(feature = "api", unix))]
use crate::_api::bind::reclaim_stale;
// The no-api arm is the only caller: with the plane there, doctor resolves through the shared
// client resolution instead, so an unconditional import would be dead in this build.
#[cfg(not(feature = "api"))]
use crate::resolve::for_doctor;
#[cfg(feature = "pg")]
use crate::resolve::pg;
use crate::{
    _core::cleanup::{Cleanup, CleanupOptions, RepairActionLevel, RepairKind},
    _fs::{
        FsCleanup, FsStorage,
        database::{Open, compact_if_needed},
    },
    _util::state::prepare_home_derived,
    Error,
    report::{Finding, PRIVATE_STATE, Report, Severity},
    resolve::{Located, Resolution},
};

/// `tinio doctor`: the shared root/mode flags plus the report's own.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "doctor", "/srv/data", "--json"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Doctor(args) = cli.command else {
///     panic!("the doctor arm");
/// };
/// assert!(args.json);
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct Args {
    /// The root and the mode, resolved as the server resolves them.
    #[command(flatten)]
    pub dir: DirArgs,
    /// Emit the report as JSON (`contracts/doctor-report.schema.json`).
    #[arg(long)]
    pub json: bool,
    /// List exactly what a fix would change, without touching anything.
    ///
    /// The pair is exclusive (`conflicts_with`): one call with one flag apart, so passing both
    /// is a usage error (exit 2) and not a silent win for either. Both flags are `api`-gated,
    /// because neither can do anything in a build with no plane to stop, no `state` file to
    /// clear and no socket to reclaim — their absence there is a compile-out, not a refusal.
    #[cfg(feature = "api")]
    #[arg(long, conflicts_with = "fix")]
    pub dry_run: bool,
    /// Apply the repairs (requires the server for the target root to be stopped).
    #[cfg(feature = "api")]
    #[arg(long)]
    pub fix: bool,
}

/// Diagnoses the directory `args` names and maps the findings onto the exit codes.
///
/// The plain run and `--dry-run` run the same check list, and `Proposals` is the whole
/// difference: a plain run's cleanup proposals are information, a dry run's are findings — so the
/// dry run lists exactly what a fix would change and exits 1 when there is something to change
/// (the doctor section's `--dry-run` sentence). `--fix` repairs first and re-checks with `Proposals::Findings`, so
/// the report holds what the repair left behind, and one `ok` row names what it applied. A live
/// control channel makes `--fix` refuse **through the report** — a `Severity::Error` finding and
/// no repair at all — never through an `Err`, so `--fix --json` stays a document a consumer can
/// parse.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::doctor::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn doctor() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args::default();
/// let code = run(args, located).await?;
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    // R8's addressing channel, captured **before** `args.dir` moves into the client resolution
    // below: `TINIO_PG_DSN` (the hidden `--pg-dsn` is its flag). A pg run has no served root, so
    // the dsn is what tells this run which database it is diagnosing, and it is read by the `pg`
    // leg alone — so the binding is gated exactly as the flags pair below is, rather than captured
    // unread in a build with no backend (the default feature set has none, and CI's lint job runs
    // `-D warnings` over it). The *other* input the check list needs — whether it is diagnosing a
    // database or a served root — is not the dsn at all but the arm the resolution took, and that
    // record travels on the [`Resolution`] the chain below builds.
    #[cfg(feature = "pg")]
    let pg_dsn = pg::named(args.dir.env.pg_dsn.clone());
    // The two flags are `api`-gated, so they are read once, here, behind the same gate:
    // everything below names the local pair and never the fields, which is what keeps this body
    // compiling in a build without the plane.
    #[cfg(feature = "api")]
    let (fix, proposals) = (
        args.fix,
        if args.dry_run {
            Proposals::Findings
        } else {
            Proposals::Informational
        },
    );
    #[cfg(not(feature = "api"))]
    let (fix, proposals) = (false, Proposals::Informational);

    // The candidate pair comes from the shared client resolution (decision 19), so `doctor` probes
    // exactly the directories `status` and `stop` probe. Without the plane there is no control
    // channel to probe at all, and the resolved directory is the only candidate.
    #[cfg(feature = "api")]
    let (resolution, candidates) = super::client_resolution(args.dir, located)?;
    #[cfg(not(feature = "api"))]
    let resolution = for_doctor(located, &args.dir)?;

    #[cfg(feature = "api")]
    let state = state_dir_for_run(&resolution, &candidates).await?;
    #[cfg(not(feature = "api"))]
    let state = state_dir_for_run(&resolution)?;

    let report = if fix {
        if state.channel_answers {
            // An answering channel — a same-version server, or a foreign-version document whose
            // recorded name still answers — is a finding, never a bare `Err`: the report is the
            // product, `--json` stays parseable, the exit code is the error finding's, and no
            // repair runs. The dial already happened inside `liveness`, including for a version
            // this build does not read.
            let mut report = Report::default();
            report.push(Finding::check(
                "control channel",
                Severity::Error,
                format!(
                    "a server is running for {}; stop it before --fix",
                    resolution.root.display()
                ),
            ));
            report
        } else {
            // Repair first, then check: with `--fix` the report holds what the repair left behind
            // (the doctor section's exit-code rule), and one summary row names what was applied. The re-check
            // runs with `Proposals::Findings`: whatever a dry run would still propose after a real
            // repair is exactly the residue the repair did not clear.
            let outcome = repair(&Run {
                resolution: &resolution,
                state: &state,
            })
            .await;
            // The repair just rewrote what is on disk — the `state` file, the socket — so the
            // re-check describes the directory **as it is now**, never the verdict the refusal was
            // taken on: the pre-repair snapshot is the refusal's input alone (U1). Re-probing
            // re-reads the recorded `control_name` too, which a cleared file no longer carries.
            let probe = liveness(&state.dir).await?;
            let repaired = StateDir {
                liveness: probe.liveness,
                control_name: probe.control_name,
                channel_answers: probe.channel_answers,
                ..state.clone()
            };
            let mut report = checks(
                &Run {
                    resolution: &resolution,
                    state: &repaired,
                },
                Proposals::Findings,
            )
            .await;
            // The stages that failed, then what was applied: `--fix` reports what the repair left
            // behind, and a stage that did not run is part of that answer rather than a reason to
            // withhold the rest of it.
            report.extend(outcome.failures);
            report.push(Finding::check(
                "repairs applied",
                Severity::Ok,
                outcome.summary,
            ));
            // `RepairKind::Full` names the home root-state-dir GC, but the fs implementation
            // defers that stage to T144, so this build cannot repair it. The row is here — in
            // `--fix`, and only in `--fix` — so the report never implies it ran. The severity is
            // a `warn` and not an `ok`: the plan's rule is that a leg which **cannot** run is
            // never silent, and "this build does not carry the stage" is that shape rather than
            // the "does not apply" one (a store-less root, a read-only run's skipped probe). It
            // costs the exit code — every `--fix` therefore exits 1 — and that is recorded in
            // `contracts/cli.md`'s exit-code rule, which states it per mode.
            report.push(Finding::check(
                "home root-state dirs",
                Severity::Warn,
                "not repaired: the home root-state-dir GC lands with T144",
            ));
            report
        }
    } else {
        checks(
            &Run {
                resolution: &resolution,
                state: &state,
            },
            proposals,
        )
        .await
    };
    // **The pg leg (ruling R8)**: a pg run has no served root — its state dir is the dsn-derived
    // synthetic root — and this arm carries no `--config`, so the instance is addressed through
    // `TINIO_PG_DSN` (the hidden `--pg-dsn` is its other channel) or `[storage.pg] dsn`. Its rows
    // are appended in every mode, plain, `--dry-run` and `--fix` alike: the check is a diagnosis,
    // and `--fix` never repairs a database (`Cleanup`'s repair is the store's own slice).
    #[cfg(feature = "pg")]
    let report = {
        let mut report = report;
        report.extend(checks::pg_findings(&resolution, pg_dsn.as_ref()).await);
        report
    };
    println!(
        "{}",
        if args.json {
            report.render_json()?
        } else {
            report.render_text()
        }
    );
    Ok(ExitCode::from(report.exit_code()))
}

/// The state dir this run uses: the resolution's own candidate first, then the other one, each
/// probed by [`liveness`] — that is plan A's `State::read` plus `client::probe`, "does anything
/// answer this channel", which is the question a repair's own guard needs. It is **not** the
/// `client::status` report `status` and `stop` answer from: a control name something else has
/// taken over answers a probe while being no tinio server at all, and `commands::probe_pair`
/// states why the two questions are not interchangeable.
///
/// The probe loop **stops at the first candidate whose channel answered** — a same-version server
/// or a foreign-version document that still dials. That candidate settles the answer, so the ones
/// behind it are neither probed nor needed. When none answers, the resolved candidate is the one
/// to diagnose and — in a `--fix` run — the one to repair: a stale `state` file and a stale socket
/// are exactly what a repair may clear *because* nothing answered.
#[cfg(feature = "api")]
pub(crate) async fn state_dir_for_run(
    resolution: &Resolution,
    candidates: &[PathBuf],
) -> Result<StateDir, Error> {
    let mut probes = Vec::with_capacity(candidates.len());
    for dir in candidates {
        let probe = liveness(dir).await?;
        let answered = probe.channel_answers;
        // Decision 19's selection is this early return: the candidate whose channel answered — a
        // same-version server or a foreign-version document that still dials — settles the answer,
        // so the ones behind it are neither probed nor needed and no scan for it is owed later.
        if answered {
            return Ok(state_dir(resolution, dir.clone(), probe));
        }
        probes.push((dir.clone(), probe));
    }
    // Nothing answered: the candidate the resolution named is the one to diagnose and — in a
    // `--fix` run — the one to repair, because a stale `state` file and a stale socket are exactly
    // what a repair may clear *because* nothing answered. The pair's first element answers when it
    // holds no such candidate.
    let index = probes
        .iter()
        .position(|(dir, _)| dir == &resolution.state_dir)
        .unwrap_or_default();
    Ok(match probes.into_iter().nth(index) {
        Some((dir, probe)) => state_dir(resolution, dir, probe),
        // `state_dir_candidates` always answers a pair; this is the empty-pair arm, answered
        // rather than unwrapped because library code does not panic.
        None => StateDir {
            dir: resolution.state_dir.clone(),
            resolved: true,
            liveness: Liveness::NoStateFile,
            control_name: None,
            channel_answers: false,
        },
    })
}

/// One probed candidate as the answer: whether it is the resolved directory, and its own liveness
/// facts.
#[cfg(feature = "api")]
fn state_dir(resolution: &Resolution, dir: PathBuf, probe: Probe) -> StateDir {
    StateDir {
        resolved: dir == resolution.state_dir,
        dir,
        liveness: probe.liveness,
        control_name: probe.control_name,
        channel_answers: probe.channel_answers,
    }
}

/// Without the plane there is no `state` file and no control channel: nothing to probe, and no
/// second candidate that could answer, so the resolved directory is the answer.
///
/// Restored by the correction round at its original body; `pub(crate)` rather than private, so the
/// unit fixture reaches the production no-api rule in that graph too.
#[cfg(not(feature = "api"))]
pub(crate) fn state_dir_for_run(resolution: &Resolution) -> Result<StateDir, Error> {
    Ok(StateDir {
        dir: resolution.state_dir.clone(),
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    })
}

/// Everything `--fix` applies, in the order it applies it, as the one line the summary finding
/// carries.
///
/// The engine's own scope is `FsCleanup::repair(RepairKind::Full)` — every startup item plus
/// meta-orphan reclamation — and the things it does not own are named at the call: the meta
/// store's integrity check and offline compact (both inside `open_store`'s `Pass::Fix`), and the
/// plane's `state`/socket reclaim (api builds, the socket half unix-only). A read-only run skips
/// the store's stages whole ([`repair_store`] says why), so there the plane's reclaim is the
/// whole of what it applies.
async fn repair(run: &Run<'_>) -> Repaired {
    let mut notes: Vec<String> = Vec::new();
    let mut failures: Vec<Finding> = Vec::new();

    let applied = repair_store(run, &mut notes, &mut failures).await;
    repair_plane(run, &mut notes, &mut failures).await;

    Repaired {
        summary: if notes.is_empty() {
            format!("{applied} repairs applied")
        } else {
            format!("{applied} repairs applied; {}", notes.join("; "))
        },
        failures,
    }
}

/// What one repair pass did, as the re-check's report takes it.
///
/// **A stage that failed is a finding, never an early return** — the same rule the check list
/// follows. The report is the product: an operator who asked `--fix` to clean a root still gets
/// every other stage's answer, the plane's leftovers are still reclaimed, and `--fix --json` is
/// a document on every path rather than an empty stdout beside one stderr line.
struct Repaired {
    /// The row the "repairs applied" summary carries: the count, plus the notes.
    summary: String,
    /// One `error` finding per stage that failed.
    failures: Vec<Finding>,
}

/// The store's stages, in the order they run: the home hardening, the store's own open (which
/// carries the integrity check), the offline compact, and the cleanup stream. Answers the number
/// of repairs applied; every failure along the way lands in `failures` and the stages behind it
/// still run.
///
/// **A read-only run has no store stages here** (FR-023 amended): its metadata source is the
/// root's own `<root>/.tinio/meta.redb`, which the storage opens read-only and never creates,
/// while the relocated tool-state dir holds tool state only — the socket, the `state` file and
/// the logs. Opening a store at the chosen dir would therefore **create** the very file the mode
/// promises not to write, and `FsStorage::new_from_db` drops the handle a line later, leaving
/// the file behind; the compaction in between would write it too. Skipping the leg whole is also
/// what keeps the home-derived tree uncreated: nothing in the repair needs it once the store's
/// stages are gone, and the plane's reclaim below only ever removes. A note carries the skip, so
/// a leg that cannot run stays visible in the summary.
async fn repair_store(
    run: &Run<'_>,
    notes: &mut Vec<String>,
    failures: &mut Vec<Finding>,
) -> usize {
    let Run { resolution, state } = *run;

    if resolution.read_only {
        notes.push(
            "read-only: the store stages were skipped; the metadata source is the root's own \
             database, opened read-only"
                .to_string(),
        );
        return 0;
    }

    // Harden the home parents first when the chosen dir is the home-derived leaf: `Pass::Fix` may
    // create the store, and a bare `create_dir_all` would skip `ensure_roots`. A failure here
    // skips the store stages rather than creating into a tree that was not hardened — which is
    // the one thing the call exists to prevent.
    if let Err(err) = prepare_home_derived(&resolution.root, &state.dir) {
        failures.push(Finding::check(
            PRIVATE_STATE,
            Severity::Error,
            format!("the home state dir could not be prepared: {err}"),
        ));
        return 0;
    }

    // The store, opened **once** for the whole repair, and never with a second handle alive: the
    // handle this leg takes is dropped before the re-check opens the store again.
    let open = match open_store(&state.dir, Pass::Fix) {
        Ok(Store::Opened { open, .. }) => open,
        // `Pass::Fix` creates, so this arm is unreachable; it is answered rather than unwrapped,
        // because this crate forbids panics in library code.
        Ok(Store::Absent) => {
            notes.push("no state store to repair".to_string());
            return 0;
        }
        Err(err) => {
            failures.push(checks::store_open_finding(&err));
            return 0;
        }
    };
    let Open {
        mut db,
        compact_needed,
        stats,
    } = open;
    // The one mapping diagnostics uses too — chosen `state.dir` pinned — so repair and the
    // re-check cannot open private state in different directories.
    let options = checks::fs_options(run);
    let access = options.access;
    if let Err(err) = compact_if_needed(
        &mut db,
        compact_needed,
        stats,
        options.compact_threshold_percent,
    ) {
        // The store is still usable: a compact that did not run leaves the marker and the rows
        // where they were, and the re-check's fragmentation row reports what it finds.
        failures.push(Finding::check(
            "meta.redb",
            Severity::Error,
            format!("the store could not be compacted: {err}"),
        ));
    }
    let storage = match FsStorage::new_from_db(&resolution.root, options, db) {
        Ok(storage) => storage,
        Err(err) => {
            failures.push(Finding::check(
                PRIVATE_STATE,
                Severity::Error,
                format!("the store could not be opened for repair: {err}"),
            ));
            return 0;
        }
    };
    let cleanup = FsCleanup::new(
        &storage,
        CleanupOptions {
            dry_run: false,
            access,
        },
    );
    // The stream is consumed asynchronously: `ActionStream<E>` is a `BoxStream`, so this is
    // `futures::StreamExt`'s iteration and not the iterator's. The count is taken while consuming
    // — the summary row needs the number of repairs, not the actions themselves — and a stage the
    // stream fails on ends the stream, which is why its `Err` is a finding rather than an abort.
    let mut stream = match cleanup.repair(RepairKind::Full).await {
        Ok(stream) => stream,
        Err(err) => {
            failures.push(Finding::check(
                PRIVATE_STATE,
                Severity::Error,
                format!("the cleanup could not be started: {err}"),
            ));
            return 0;
        }
    };
    let mut applied = 0usize;
    while let Some(action) = stream.next().await {
        match action {
            // The report-only stages ride the fix pass too (the block check), and their rows are
            // `Lint` — "a check finding, no action needed". Counting one as a repair would put a
            // finding in the "N repairs applied" summary; the level is what keeps the ledger a
            // ledger.
            Ok(action) if action.level != RepairActionLevel::Lint => applied += 1,
            Ok(_) => {}
            Err(err) => failures.push(Finding::check(
                PRIVATE_STATE,
                Severity::Error,
                format!("a repair stage failed: {err}"),
            )),
        }
    }
    applied
}

/// The plane's own leftovers: the `state` file and the control socket (api builds; the socket
/// half is unix-only). The socket's path is taken **before** the state file goes, and it is taken
/// from the run's own probe (`socket_path_of`): the name the `state` file recorded is the
/// channel's, and re-deriving it here would unlink a path that was never the socket while leaving
/// the stale one behind.
#[cfg(feature = "api")]
async fn repair_plane(run: &Run<'_>, notes: &mut Vec<String>, failures: &mut Vec<Finding>) {
    let Run { state, .. } = *run;

    // A configuration `api_options` refuses is a note, never an abort: the rest of the repair
    // still runs and the re-check's socket row reports the refusal as "not inspected".
    #[cfg(unix)]
    let socket = match socket_path_of(run) {
        Ok(socket) => Some(socket),
        Err(err) => {
            notes.push(format!("the control socket could not be resolved: {err}"));
            None
        }
    };

    // **The dial is re-taken here, immediately before anything is cleared.** `state.liveness` was
    // probed before every store stage above — the integrity check, the open, the compaction, the
    // cleanup stream — which is seconds of work on a large store, and the `state` file is how
    // `status` and `stop` find the channel. A server that started inside that window must not have
    // its `state` file removed from under it.
    let answered = match liveness(&state.dir).await {
        // `liveness` dials every decoded document, including a version this build does not read,
        // so this re-check is the same question as the refusal above — taken again, immediately
        // before the file goes, in case a server started during the store stages.
        Ok(probe) => probe.channel_answers,
        // A channel that cannot be probed is not a channel proven silent, so nothing is cleared.
        // `liveness` folds every `State::read` failure into `Unreadable`, so this is a floor
        // rather than a path.
        Err(err) => {
            failures.push(Finding::check(
                "control channel",
                Severity::Error,
                format!("the control channel could not be probed: {err}"),
            ));
            true
        }
    };
    if answered {
        // Nothing is cleared: the repair's own stages have run, but the plane's leftovers are not
        // leftovers after all, and the re-check reports the live channel.
        notes.push("the control channel answered; the `state` file was left alone".to_string());
        return;
    }

    if let Err(err) = State::remove(&state.dir) {
        failures.push(Finding::check(
            "state file",
            Severity::Error,
            format!("the `state` file could not be removed: {err}"),
        ));
    }
    #[cfg(unix)]
    if let Some(socket) = socket {
        // The same call the server makes at startup, with `dry_run: false`, on a channel the dial
        // above has just found silent. A failure means a listener answered after that dial, which
        // the re-check's own socket row reports as "not inspected" — so it is logged and noted
        // here rather than pushed as a second finding about the same channel.
        if let Err(err) = reclaim_stale(&socket, false) {
            notes.push("the control socket was left alone".to_string());
            tracing::warn!(error = %err, "the control socket was left alone");
        }
    }
}

/// The plane's leftovers on a build without it: there is no `state` file and no control channel
/// to reclaim, so the pass has nothing to do.
#[cfg(not(feature = "api"))]
async fn repair_plane(_run: &Run<'_>, _notes: &mut Vec<String>, _failures: &mut Vec<Finding>) {}

mod audit;
mod checks;
#[cfg(test)]
mod tests;

#[cfg(all(feature = "api", unix))]
pub use self::checks::socket_path_of;
pub use self::checks::{
    DEFAULT_FLOOR_BYTES, Liveness, Pass, Probe, Proposals, Run, StateDir, Store, below_floor,
    checks, free_space, liveness, open_store,
};
