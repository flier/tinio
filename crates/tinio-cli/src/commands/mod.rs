//! The five command implementations: `start` (server/start), `status`, `stop`, `doctor` and
//! the `service` verbs' driver.
//!
//! Only `status` and `stop` are compiled out with the `api` feature — `doctor` diagnoses the
//! always-present layers, so it exists in both builds and only its plane-dependent half is
//! gated (`cli::Command`'s doc states the rule). Each module owns its own resolution through
//! `resolve`, so no command re-derives a root, a mode or a state directory.
//!
//! **The two client commands share one resolution and one probe** (`client_resolution` — the
//! gate-independent body `status`, `stop`, `doctor` and the unit fixture all call — and
//! `probe_pair`, the probe that follows it): decision 19's rule is that a client looks where the
//! server would leave its state *and* at the other candidate, so a second implementation of
//! either half is exactly the drift the rule exists to prevent.

pub mod doctor;
pub mod start;
#[cfg(feature = "api")]
pub mod status;
#[cfg(feature = "api")]
pub mod stop;

use std::path::PathBuf;

use clap::{ArgAction, Args};

#[cfg(feature = "api")]
use crate::_api::client::{self, Report};
use crate::_config::state::parse_state_dir;
#[cfg(any(feature = "api", test))]
use crate::{
    _config::state,
    Error,
    resolve::{self, Located, Resolution},
};

/// The environment-only channels the resolution reads: `TINIO_STATE_DIR` and the `[auth]` pair.
///
/// **Every field is `hide = true`.** The environment is the documented channel for all three
/// (`contracts/config.md:110`), and a secret typed on a command line is visible in the process
/// table — so `--secret-key` exists only as the carrier of its variable, and the flag column of
/// either contract is unchanged.
///
/// `service` flattens this struct too, because its own channels are `TINIO_READ_ONLY` and
/// `TINIO_STATE_DIR` and the flag has one author rather than two; it reads neither credential,
/// which is what it does today as well: the command talks to the supervisor and reads no
/// configuration file.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::commands::EnvArgs;
///
/// // Nothing in the environment: every channel unset, which is what the resolution reads as
/// // "the config file decides".
/// assert!(EnvArgs::default().state_dir.is_none());
/// ```
#[derive(Debug, Default, Args)]
pub struct EnvArgs {
    /// The state directory (`TINIO_STATE_DIR`, FR-023): an explicit location used verbatim
    /// instead of the `~/.tinio/roots/<hash>/` derivation — the escape hatch for the
    /// deployments read-only mode is for (service accounts, containers, immutable images),
    /// where the home may be absent or unwritable. Its parents are not created mode 0700 and no
    /// symlink check applies, because the caller chose the location.
    #[arg(
        long = "state-dir",
        env = "TINIO_STATE_DIR",
        value_name = "PATH",
        hide = true,
        value_parser = parse_state_dir,
    )]
    pub state_dir: Option<PathBuf>,
    /// The `[auth]` access key. The pair is set together or not at all — half of it is a
    /// configuration failure, never a run that keeps the file's secret key and silently drops
    /// the operator's access key (`resolve::server`'s fold is where that refusal lives).
    #[arg(
        long = "access-key",
        env = "TINIO_ACCESS_KEY",
        value_name = "KEY",
        hide = true
    )]
    pub access_key: Option<String>,
    /// The `[auth]` secret key; see [`EnvArgs::access_key`] for the pair's rule. The `MINIO_*`
    /// names are deliberately absent: `contracts/config.md:110` scopes them to the data plane's
    /// own fallback (T084), and one variable with two owners has two precedence positions.
    #[arg(
        long = "secret-key",
        env = "TINIO_SECRET_KEY",
        value_name = "KEY",
        hide = true
    )]
    pub secret_key: Option<String>,
}

/// The arguments `status`, `stop` and `doctor` share: a root and the mode.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "doctor", "/srv/data", "--read-only"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Doctor(args) = cli.command else {
///     panic!("the doctor arm");
/// };
/// assert!(args.dir.read_only);
/// ```
#[derive(Debug, Default, Args)]
pub struct DirArgs {
    /// The storage root; absent: walk up from the current directory to the nearest ancestor
    /// containing `.tinio/`.
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Resolve the mode exactly as the server does (config `[server] read_only`, env
    /// `TINIO_READ_ONLY`).
    #[arg(
        long,
        env = "TINIO_READ_ONLY",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true,
        action = ArgAction::Set,
        default_value_t = false,
    )]
    pub read_only: bool,
    /// The environment-only channels: the state-dir override and the credential pair.
    #[command(flatten)]
    pub env: EnvArgs,
}

/// The boolean switches' parser: `tinio_util::switch::parse_switch`, the tolerant
/// `1`/`true`/`0`/`false` spelling and **the one definer of it in this workspace**.
///
/// **Re-exported rather than re-spelled, because the flag and the variable are one decision on two
/// channels.** `TINIO_READ_ONLY`/`TINIO_SANDBOX` are parsed from the environment by `tinio-config`
/// (`sources.rs`), and the same two names reach the same flags through their `env` attributes
/// here; while each side carried its own `match`, the four spellings and the refusal's wording were
/// equal exactly as long as an author remembered the other copy existed, and the answer decides
/// whether the process protects its root or confines itself.
///
/// **This is what lets `--read-only` and `--sandbox` carry an `env` attribute at all.** clap's
/// `SetTrue` action parses its values with `ValueParser::bool()`, whose only spellings are the
/// literals `true` and `false` — so `TINIO_READ_ONLY=1` (the spelling `contracts/config.md:110`
/// documents and every harness sets) would die in the parser with "invalid value '1'" before a
/// line of this crate's code ran. The switches therefore take `ArgAction::Set` with
/// `num_args(0..=1)` and `default_missing_value("true")`: the bare flag still means `true`, the
/// variable is read by clap like any other, and both go through this one parser.
///
/// The strictness is deliberate and matches the contract: a typo must not silently switch a
/// confinement off (nor serve a writable root), so anything but the four spellings is refused —
/// an empty value included, which is never "unset" and never `false`.
pub(crate) use crate::_util::switch::parse_switch;

/// The root, the mode and the ordered state-dir candidates a client command probes — resolved
/// **once**, for `status`, `stop` and (through the same chain) plan C's `doctor`, and reached
/// directly by the unit fixture too, so the command and the fixture cannot drift.
///
/// The chain is `resolve::for_doctor`, on the [`Located`] `lib.rs` resolved — the **client-side**
/// resolution, not the server's:
/// `status` and `stop` launch nothing, and a root whose configuration does not parse is a state
/// to report rather than a reason to refuse to report it. The function's name is `doctor`'s and
/// the chain is every non-launching command's, which is what keeps decision 19's resolution one
/// implementation and not three; the load outcome it records beside the config is carried
/// unread here, because a client reports state and does not diagnose it.
///
/// The candidates are [`resolve::state_dir_candidates`]'s five-argument rule: the resolved
/// directory first, then the **other** one — `<root>/.tinio` for a read-only run and the
/// home-derived directory for a read-write one, with `TINIO_STATE_DIR` replacing that
/// home-derived candidate whenever it is set. **The override is the discovery parse's, and it is
/// read off the resolution it produced** ([`Resolution::state_dir_override`], carried there
/// rather than taken off [`Located`] before the move): the file its own directory holds was
/// located from that value, and a second read could answer differently (the `.env` load happens
/// between the two parses). `default_home_state_dir_path` is the **non-creating** form of the
/// derivation: a client asks whether a server is there, so it must not create the home tree it
/// is asking about — and the lazy closure is what keeps an exported `TINIO_STATE_DIR` from
/// touching the home at all (`state_dir_candidates` takes the derivation lazily for exactly that
/// reason).
///
/// The gate is `any(feature = "api", test)`: no-api builds have no client command to resolve for,
/// but the `test` graph's fixture does, and the lib target of a no-api check would otherwise see
/// the function as dead code under `-D warnings`.
///
/// # Errors
///
/// A root that does not exist or cannot be canonicalized (exit 2, the one usage error this path
/// has), a `TINIO_STATE_DIR` that is set but empty (the parse refuses it, exit 2 as well), and a
/// state dir that cannot be derived in read-only mode with no home to derive it from.
#[cfg(any(feature = "api", test))]
pub(crate) fn client_resolution(
    args: DirArgs,
    located: Located,
) -> Result<(Resolution, Vec<PathBuf>), Error> {
    let resolution = resolve::for_doctor(located, &args)?;
    let candidates = resolve::state_dir_candidates(
        &resolution.root,
        &resolution.state_dir,
        resolution.read_only,
        resolution.state_dir_override.clone(),
        || Ok(state::default_home_state_dir_path(&resolution.root)?),
    )?;
    Ok((resolution, candidates))
}

/// What a client command learned from the candidate pair: the directory the answer came from,
/// and that directory's own report.
///
/// `state_dir` is the candidate a server answered in, or — when none did — the pair's **first**
/// element, the directory a server for this root would use. `report` belongs to that directory
/// either way, which is what lets a stopped root say *which* stopped shape it is: no `state`
/// file at all, or one nothing answers for.
#[cfg(feature = "api")]
pub(crate) struct Answer {
    /// The candidate this answer came from.
    pub(crate) state_dir: PathBuf,
    /// That candidate's own report (`client::status`'s).
    pub(crate) report: Report,
}

#[cfg(feature = "api")]
impl Answer {
    /// Whether a server answered here.
    ///
    /// The one gate both commands read, so `status`'s exit code and `stop`'s refusal cannot
    /// disagree about what "running" means.
    pub(crate) fn running(&self) -> bool {
        self.report.running
    }
}

/// Probes the pair in order and answers with the first candidate a server is live in — the
/// resolved directory first, the other one only when nothing is there (decision 19).
///
/// `client::status` is the question, not `client::probe`: a control name that something else has
/// taken over answers a probe while being no tinio server at all, and both commands need the
/// report anyway — `status` prints it, `stop` refuses on it.
///
/// # Errors
///
/// An unreadable `state` file, on any candidate, is the one failure a probe can have
/// (the `status` section's stopped/unreadable rule: a missing file and a failed probe are both "stopped", an unreadable
/// one is an error). It is reported rather than skipped: a file this tool wrote and cannot read
/// is the operator's finding, not something to paper over by answering from the other candidate.
#[cfg(feature = "api")]
pub(crate) async fn probe_pair(candidates: &[PathBuf]) -> Result<Answer, Error> {
    let mut stopped: Option<Answer> = None;
    for dir in candidates {
        let report = client::status(dir).await?;
        let answer = Answer {
            state_dir: dir.clone(),
            report,
        };
        if answer.running() {
            return Ok(answer);
        }
        // The first candidate's report is the one a stopped run has to answer with — the second
        // candidate's would describe a directory the root does not use.
        if stopped.is_none() {
            stopped = Some(answer);
        }
    }
    // Unreachable in practice — `state_dir_candidates` always answers with two directories — and
    // an error rather than a panic, because the pair is not this function's to know the length of.
    stopped.ok_or_else(|| Error::Operational("no state directory to probe".into()))
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    use crate::{
        _config::state::STATE_DIR_VAR,
        _util::testing::assert_send_sync,
        cli::{ACCESS_KEY_VAR, Cli, SECRET_KEY_VAR},
    };

    #[test]
    fn the_switches_parse_their_tolerant_spelling() {
        // The parser is what lets the switches carry a variable at all: the four spellings the
        // contract documents, the trimmed form included, and a refusal for anything else — an
        // empty value is an error, never "unset" and never `false`.
        for raw in ["1", "true", " 1 "] {
            assert!(parse_switch(raw).expect("parses"), "{raw:?}");
        }
        for raw in ["0", "false"] {
            assert!(!parse_switch(raw).expect("parses"), "{raw:?}");
        }
        for raw in ["", "yes", "TRUE"] {
            let err = parse_switch(raw).expect_err("refused");
            assert!(err.contains("0/1/true/false"), "{raw:?}: {err}");
        }
    }

    /// The environment-only channels are **hidden everywhere they appear**, which is what makes
    /// the contracts' flag columns true: the environment is the documented channel, and a secret
    /// typed on a command line is visible in the process table.
    #[test]
    fn the_environment_only_channels_are_hidden_on_every_arm() {
        for command in Cli::command().get_subcommands() {
            for arg in command.get_arguments() {
                let hidden_channel = matches!(
                    arg.get_env().map(|name| name.to_string_lossy().into_owned()),
                    Some(name)
                        if name == STATE_DIR_VAR
                            || name == ACCESS_KEY_VAR
                            || name == SECRET_KEY_VAR
                );
                if hidden_channel {
                    assert!(
                        arg.is_hide_set(),
                        "`{}` carries an environment-only channel and must be hidden",
                        arg.get_id()
                    );
                }
            }
        }
    }

    #[test]
    fn the_public_surface_is_send_and_sync() {
        assert_send_sync::<DirArgs>();
        assert_send_sync::<EnvArgs>();
    }
}
