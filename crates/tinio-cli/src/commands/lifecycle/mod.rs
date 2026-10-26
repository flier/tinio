//! `tinio lifecycle` — the four verbs that configure a bucket's lifecycle policy through a
//! running server's management plane (`contracts/cli.md` §`tinio lifecycle`).
//!
//! **FR-007's one exception to "no data commands", and it stays inside it**: the verbs speak the
//! plane's three `/buckets/{bucket}/lifecycle` routes and nothing else — no S3 client, no storage
//! backend, no bucket and no object touched — so a root with no server answers the usual
//! no-server refusal rather than an empty configuration.
//!
//! **The plane is the request path** (LC8): a rule the model refuses is refused by the server, and
//! what prints is the plane's own sentence — `{"rule","reason"}` for a refused configuration,
//! `{"error"}` for every other refusal. Nothing here re-validates a document or re-reads a status
//! into a second verdict.
//!
//! The group exists only in a build carrying this crate's `lifecycle` feature, which is what turns
//! on the plane's routes (`management-api.md` §Lifecycle configuration): without it the
//! subcommand is not in the parser at all, as `status` and `stop` are not without `api`.

pub mod add;
pub mod ls;
pub mod put;
pub mod rm;

#[cfg(test)]
use std::path::Path;
use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};
use http::Method;

pub use super::DirArgs;
use crate::{
    _api::{
        ErrorBody, State,
        client::{self, RawResponse},
        router::InvalidBody,
    },
    _core::{bucket, lifecycle},
    Error,
    resolve::Located,
};

/// The `tinio lifecycle` subcommand: one of the contract's four verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "lifecycle", "ls", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::Lifecycle(_)));
/// ```
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The verb and its own arguments.
    #[command(subcommand)]
    pub verb: Verb,
}

/// The contract's four verbs, each with the arguments its own row lists.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::lifecycle::Verb,
/// };
///
/// fn verb(argv: &[&str]) -> Verb {
///     let matches = Cli::command().try_get_matches_from(argv).expect("parse");
///     let cli = Cli::from_arg_matches(&matches).expect("from matches");
///     let Command::Lifecycle(args) = cli.command else {
///         panic!("the lifecycle arm");
///     };
///     args.verb
/// }
///
/// assert!(matches!(
///     verb(&["tinio", "lifecycle", "ls", "photos"]),
///     Verb::Ls(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "lifecycle", "add", "photos", "--id", "logs"]),
///     Verb::Add(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "lifecycle", "rm", "photos"]),
///     Verb::Rm(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "lifecycle", "put", "photos", "config.json"]),
///     Verb::Put(_)
/// ));
/// ```
#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Print the bucket's stored configuration.
    Ls(ls::Args),
    /// Upsert one rule by `--id`.
    Add(add::Args),
    /// Delete the bucket's whole configuration.
    Rm(rm::Args),
    /// Replace the whole configuration with the model read from `FILE`.
    Put(put::Args),
}

impl Args {
    /// The root, the mode and the environment channels every verb flattens: the discovery parse
    /// reads them for all four through this one accessor, so no verb can resolve one root and
    /// then ask a server that serves another.
    pub(crate) fn dir_args(&self) -> &DirArgs {
        match &self.verb {
            Verb::Ls(args) => &args.dir,
            Verb::Add(args) => &args.dir,
            Verb::Rm(args) => &args.dir,
            Verb::Put(args) => &args.dir,
        }
    }
}

/// Runs the verb the command line named.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), the refusal a root with nothing running answers
/// (exit 1, the sentence `stop` gives), a plane refusal in the plane's own words (exit 1), and —
/// for `put` — a file that cannot be read or does not parse (exit 2).
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::lifecycle::{Args, Verb, ls, run},
///     resolve::locate,
/// };
///
/// # async fn print() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     verb: Verb::Ls(ls::Args {
///         bucket: "photos".into(),
///         dir: Default::default(),
///     }),
/// };
/// let code = run(args, located).await?;
/// // 0 once the configuration has printed; a refusal is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// The example is `no_run` because it needs a running server; a live call prints the bucket's
/// stored configuration and exits 0.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    match args.verb {
        Verb::Ls(args) => ls::run(args, located).await,
        Verb::Add(args) => add::run(args, located).await,
        Verb::Rm(args) => rm::run(args, located).await,
        Verb::Put(args) => put::run(args, located).await,
    }
}

/// A connected management plane: one root resolution and one route target, so
/// the four verbs cannot ask different servers or spell the route differently.
///
/// The bucket is validated with the contract's own parser: a name it refuses
/// is a usage error, so the plane is never asked to route a name S3 cannot
/// spell.
pub(crate) struct Plane {
    state: State,
    target: String,
    supports_tiers: bool,
}

impl Plane {
    /// Resolve the server and the route for `bucket`.
    pub(crate) async fn connect(
        args: DirArgs,
        located: Located,
        bucket: &str,
    ) -> Result<Self, Error> {
        let Ok(bucket) = bucket::name(bucket.to_owned()) else {
            return Err(Error::Usage(format!("invalid bucket name: {bucket}")));
        };
        let (state, supports_tiers) = super::server_state_with_tiers(args, located).await?;
        Ok(Self {
            state,
            target: format!("/buckets/{bucket}/lifecycle"),
            supports_tiers,
        })
    }

    /// Whether the served root's backend stores objects in tiers (T11).
    pub(crate) fn supports_tiers(&self) -> bool {
        self.supports_tiers
    }

    /// The tier gate both write verbs apply before their `PUT` (T11), over this plane's own
    /// capability.
    ///
    /// This method is what the verbs call, so the link between the plane's answer and the
    /// refusal is the tested unit: a test drives it with either capability without a live
    /// server.
    pub(crate) fn refuse_transitions(
        &self,
        bucket: &str,
        config: &lifecycle::Config,
    ) -> Result<(), Error> {
        refuse_transitions(bucket, config, self.supports_tiers())
    }

    /// One call over the plane's route.
    pub(crate) async fn call(
        &self,
        method: Method,
        body: Option<&str>,
    ) -> Result<RawResponse, Error> {
        client::send(&self.state, &self.target, method, body)
            .await
            .map_err(Into::into)
    }
}

/// A plane fixture for the verbs' gate tests: a control channel no test calls, and the
/// capability the gate reads.
#[cfg(test)]
pub(crate) fn test_plane(supports_tiers: bool) -> Plane {
    Plane {
        state: State::new(
            "tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("the fixture state"),
        target: "/buckets/photos/lifecycle".to_owned(),
        supports_tiers,
    }
}

/// The tier gate the two write verbs apply before their `PUT` (T11).
///
/// A backend without tiers refuses a configuration carrying a transition in the same sentence
/// [`refusal`] prints for the plane's `400 {"rule","reason"}` — the reason every write path
/// shares, with the rule's ID leading when it has one — so an operator sees one answer
/// whichever surface refused. [`Plane::refuse_transitions`] is the verbs' door to it.
fn refuse_transitions(
    bucket: &str,
    config: &lifecycle::Config,
    supports_tiers: bool,
) -> Result<(), Error> {
    if supports_tiers {
        return Ok(());
    }
    let Some(rule) = config.transition_rule() else {
        return Ok(());
    };
    // The same sentence [`refusal`] prints for the plane's `400 {"rule","reason"}`: the reason
    // is [`TRANSITIONS_NEED_TIERS`](lifecycle::TRANSITIONS_NEED_TIERS), and the rule's ID leads
    // when it has one.
    Err(Error::Operational(match &rule.id {
        Some(id) => format!(
            "lifecycle {bucket}: rule {id}: {}",
            lifecycle::TRANSITIONS_NEED_TIERS
        ),
        None => format!("lifecycle {bucket}: {}", lifecycle::TRANSITIONS_NEED_TIERS),
    }))
}

/// The status a verb requires, or the plane's own refusal sentence.
pub(crate) fn require_status(bucket: &str, answer: &RawResponse, status: u16) -> Result<(), Error> {
    if answer.status != status {
        return Err(refusal(bucket, answer));
    }
    Ok(())
}

/// The answer a verb could not use, in the plane's own words.
///
/// **The two shapes the contract gives**: a refused configuration is `400 {"rule","reason"}` and
/// prints the offending rule's ID beside the model's reason — the reason alone when the
/// configuration itself is what was refused — while every other refusal is `{"error"}`
/// (`no lifecycle configuration`, `no such bucket`, `lifecycle is not served here`,
/// `access denied`) and prints as it came. Nothing is re-decided here: which status means what is
/// the plane's (LC8).
pub(crate) fn refusal(bucket: &str, raw: &RawResponse) -> Error {
    let plane = serde_json::from_str::<InvalidBody>(&raw.body)
        .map(|invalid| match invalid.rule {
            Some(id) => format!("rule {id}: {}", invalid.reason),
            None => invalid.reason,
        })
        .or_else(|_| serde_json::from_str::<ErrorBody>(&raw.body).map(|body| body.error))
        .unwrap_or_else(|_| raw.body.trim().to_string());
    Error::Operational(format!("lifecycle {bucket}: {plane}"))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::{CommandFactory, FromArgMatches};

    use super::*;
    use crate::cli::{Cli, Command};

    /// The parse as the process does it: clap's matches, then the `Cli` the dispatch reads.
    fn parse(argv: &[&str]) -> Result<Command, String> {
        let matches = Cli::command()
            .try_get_matches_from(argv)
            .map_err(|err| err.to_string())?;
        Cli::from_arg_matches(&matches)
            .map(|cli| cli.command)
            .map_err(|err| err.to_string())
    }

    /// The lifecycle arm, or a panic naming what parsed instead.
    fn lifecycle(argv: &[&str]) -> Verb {
        let command = parse(argv).expect("the command line parses");
        let Command::Lifecycle(args) = command else {
            panic!("{argv:?} is not the lifecycle arm");
        };
        args.verb
    }

    /// The contract's four command lines, and the arguments each verb's row lists.
    #[test]
    fn the_four_verbs_parse() {
        let Verb::Ls(ls) = lifecycle(&["tinio", "lifecycle", "ls", "photos"]) else {
            panic!("the ls verb");
        };
        assert_eq!(ls.bucket, "photos");

        let Verb::Add(add) = lifecycle(&[
            "tinio",
            "lifecycle",
            "add",
            "photos",
            "--id",
            "logs",
            "--prefix",
            "logs/",
            "--expire-days",
            "30",
            "--noncurrent-days",
            "7",
            "--abort-days",
            "3",
        ]) else {
            panic!("the add verb");
        };
        assert_eq!(add.bucket, "photos");
        assert_eq!(add.id, "logs");
        assert_eq!(add.prefix.as_deref(), Some("logs/"));
        assert_eq!(add.expire_days, Some(30));
        assert_eq!(add.noncurrent_days, Some(7));
        assert_eq!(add.abort_days, Some(3));

        let Verb::Rm(rm) = lifecycle(&["tinio", "lifecycle", "rm", "photos"]) else {
            panic!("the rm verb");
        };
        assert_eq!(rm.bucket, "photos");

        let Verb::Put(put) = lifecycle(&["tinio", "lifecycle", "put", "photos", "config.json"])
        else {
            panic!("the put verb");
        };
        assert_eq!(put.bucket, "photos");
        assert_eq!(put.file, Path::new("config.json"));
    }

    /// `--id` is the verb's identity: `add` without one names no rule to upsert.
    #[test]
    fn add_requires_an_id() {
        assert!(parse(&["tinio", "lifecycle", "add", "photos"]).is_err());
        assert!(
            parse(&["tinio", "lifecycle", "add", "photos", "--id", "logs"]).is_ok(),
            "the id alone is enough — the plane refuses a rule with no action, not the parser"
        );
    }

    /// `put` replaces the configuration from a file, so the path is not optional.
    #[test]
    fn put_requires_a_path() {
        assert!(parse(&["tinio", "lifecycle", "put", "photos"]).is_err());
        assert!(parse(&["tinio", "lifecycle", "put", "photos", "config.json"]).is_ok());
    }

    /// The plane's two refusal shapes print as the plane spelled them: a refused configuration
    /// names the rule and the reason, and every other refusal is its own sentence.
    #[test]
    fn a_refusal_prints_the_planes_own_sentence() {
        let invalid = RawResponse {
            status: 400,
            body: r#"{"rule":"expire-logs","reason":"a rule carries at least one action"}"#.into(),
        };
        assert_eq!(
            refusal("photos", &invalid).to_string(),
            "lifecycle photos: rule expire-logs: a rule carries at least one action"
        );

        // The configuration's own refusal carries no rule ID.
        let invalid = RawResponse {
            status: 400,
            body: r#"{"rule":null,"reason":"a configuration carries at least one rule"}"#.into(),
        };
        assert_eq!(
            refusal("photos", &invalid).to_string(),
            "lifecycle photos: a configuration carries at least one rule"
        );

        let absent = RawResponse {
            status: 404,
            body: r#"{"error":"no lifecycle configuration"}"#.into(),
        };
        assert_eq!(
            refusal("photos", &absent).to_string(),
            "lifecycle photos: no lifecycle configuration"
        );
    }
}
