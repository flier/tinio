//! `tinio object-lock` and the machinery the three Object Lock groups share
//! (`contracts/cli.md` §`tinio object-lock`, §`tinio retention`, §`tinio legalhold`).
//!
//! **The CLI drives the plane and nothing else** (OL3): every verb speaks the two
//! `/buckets/{bucket}/object-lock` routes, and no store handle and no S3 client enters this
//! crate. A root with no server therefore answers the usual no-server refusal rather than an
//! empty configuration.
//!
//! **The plane is the request path** (OL30): a rule the model refuses is refused by the server,
//! and what prints is the plane's own sentence (`{"error": …}`). The `--json` shapes are an
//! output rendering of `mc`'s field names, not a second stored wire.
//!
//! **The version address always travels in the JSON body** (OL29), never as a path segment; a
//! missing `--version-id` is `current`, never an absent address.
//!
//! The groups exist only in a build carrying this crate's `object-lock` feature (which requires
//! `api`), like the plane's routes: without it the subcommands are not in the parser at all.

pub mod enable;

#[cfg(test)]
mod tests;

use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};

pub use super::DirArgs;
use crate::{
    _api::object_lock::{ObjectAddress, UpdateObjectLock, VersionAddress},
    _core::object_lock as model,
    Error,
    resolve::Located,
};

/// The `tinio object-lock` subcommand: the contract's one verb.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "object-lock", "enable", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::ObjectLock(_)));
/// ```
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The verb and its own arguments.
    #[command(subcommand)]
    pub verb: Verb,
}

/// The contract's one `object-lock` verb.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::object_lock::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "object-lock", "enable", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::ObjectLock(args) = cli.command else {
///     panic!("the object-lock arm");
/// };
/// assert!(matches!(args.verb, Verb::Enable(_)));
/// ```
#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Enable Object Lock on a bucket.
    Enable(enable::Args),
}

impl Args {
    /// The root, the mode and the environment channels the verb flattens.
    pub(crate) fn dir_args(&self) -> &DirArgs {
        match &self.verb {
            Verb::Enable(args) => &args.dir,
        }
    }
}

/// Runs the verb the command line named.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), the refusal a root with nothing running answers
/// (exit 1, the sentence `stop` gives), and the plane's refusal in its own words (exit 1).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::object_lock::{Args, Verb, enable, run},
///     resolve::locate,
/// };
///
/// # async fn enable_lock() -> Result<(), tinio_cli::Error> {
/// let root = tempfile::TempDir::new().expect("a scratch root");
/// let discovery = Discovery {
///     dir: Some(root.path().to_path_buf()),
///     config: None,
///     read_only: false,
///     state_dir: Some(root.path().join("state")),
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     verb: Verb::Enable(enable::Args {
///         bucket: "photos".into(),
///         output: Default::default(),
///         dir: Default::default(),
///     }),
/// };
/// let error = run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(enable_lock()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    match args.verb {
        Verb::Enable(args) => enable::run(args, located).await,
    }
}

/// The object route's request/answer shape is the plane's own `UpdateObjectLock` (one DTO, OL30):
/// its serializer omits an absent member and emits `null` for an explicit clear, which is exactly
/// the operator's "leave" versus "clear" distinction (OL23).
///
/// [`object_update`] is the CLI's one constructor: the address plus whichever members the verb
/// sets.
pub(crate) fn object_update(
    key: &str,
    version_id: Option<&str>,
    retention: Option<Option<model::Retention>>,
    legal_hold: Option<model::LegalHold>,
    bypass: bool,
) -> UpdateObjectLock {
    UpdateObjectLock {
        key: key.to_owned(),
        version: address(version_id),
        retention,
        legal_hold,
        bypass,
    }
}

/// OL29's three address spellings, as the CLI can express them: no `--version-id` is `current`,
/// and an id is the id. The null version is not a CLI spelling (`contracts/cli.md`).
pub(crate) fn address(version_id: Option<&str>) -> VersionAddress {
    match version_id {
        Some(id) => VersionAddress::Id(id.to_owned()),
        None => VersionAddress::Current,
    }
}

/// The object route's `GET` body.
pub(crate) fn object_address(key: &str, version_id: Option<&str>) -> ObjectAddress {
    ObjectAddress {
        key: key.to_owned(),
        version: address(version_id),
    }
}
