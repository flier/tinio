//! `tinio legalhold clear` — clear a legal hold to OFF.

use std::process::ExitCode;

pub use super::HoldArgs as Args;
#[cfg(test)]
use crate::commands::plane::{Outcome, Plane};
use crate::{_core::object_lock as model, Error, resolve::Located};

/// Sets the addressed version's legal hold to `OFF`.
///
/// There is no `--bypass`: a legal hold is cleared by clearing it, and a retention's governance
/// authority says nothing about it (OL11).
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a target with no key (exit 2), no running server
/// (exit 1), and the plane's refusal in its own words (exit 1).
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command, Discovery, Provenance},
///     commands::legalhold::{self, Verb},
///     resolve::locate,
/// };
///
/// # async fn clear() -> Result<(), tinio_cli::Error> {
/// let root = tempfile::TempDir::new().expect("a scratch root");
/// let discovery = Discovery {
///     dir: Some(root.path().to_path_buf()),
///     config: None,
///     read_only: false,
///     state_dir: Some(root.path().join("state")),
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "legalhold", "clear", "photos/report.txt"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::LegalHold(args) = cli.command else {
///     panic!("the legalhold arm");
/// };
/// assert!(matches!(args.verb, Verb::Clear(_)));
/// let error = legalhold::run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(clear()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    super::run_hold(args, located, model::LegalHold::Off, "legalhold clear").await
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute<P: Plane>(args: &Args, plane: &P) -> Result<Outcome, Error> {
    super::execute_hold(args, plane, model::LegalHold::Off, "legalhold clear").await
}
