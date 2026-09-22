//! `tinio doctor` — offline diagnostics of a directory, with no server required.
//!
//! **A stub in this task**: plan C's Task 1 replaces it with the report type, its two
//! renderings and the checks. The module compiles in **both** builds — `cli::Command`'s doc
//! states why — and only the items that need the plane are `#[cfg(feature = "api")]`, because
//! without a control channel the command cannot prove a server is stopped and must not offer a
//! repair it cannot guard.

use std::process::ExitCode;

use clap::Args as ClapArgs;

use super::DirArgs;
use crate::{Error, resolve::Located};

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
    #[arg(long)]
    pub dry_run: bool,
    /// Apply the repairs (requires the server for the target root to be stopped).
    #[arg(long)]
    pub fix: bool,
}

/// Diagnoses the directory `args` names and maps the findings onto the exit codes.
///
/// *A stub in this task.*
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
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args::default();
/// let code = run(args, located).await?;
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// **The pair is what plan C's body will read**: `located` is decision 19's resolution — the
/// root, the mode and the state dir, resolved once for every non-launching command — and the
/// arguments are the authoritative parse's, so a `.env`-supplied credential pair reaches the
/// report exactly as it reaches the server. Neither is read by the stub.
pub async fn run(_args: Args, _located: Located) -> Result<ExitCode, Error> {
    Err(Error::Usage("not implemented yet".into()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{Args, DirArgs};
    use crate::_util::testing::assert_send_sync;

    impl Args {
        /// An `Args` naming `root` and nothing else.
        pub(crate) fn for_test(root: &Path) -> Self {
            Self {
                dir: DirArgs {
                    dir: Some(root.to_path_buf()),
                    ..DirArgs::default()
                },
                ..Self::default()
            }
        }
    }

    #[test]
    fn the_public_surface_is_send_and_sync() {
        assert_send_sync::<Args>();
    }
}
