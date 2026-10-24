//! `tinio retention set` — set a version's retention or the bucket's default rule.

use std::{mem, process::ExitCode};

use super::{
    DirArgs, OutputArgs, RetentionMessage, Validity, default_rule, format_instant, parse_mode,
    parse_validity, retain_until, set_update, write_config,
};
use crate::{
    _core::object_lock as model,
    Error,
    commands::plane::{
        Connection, Outcome, Plane, Target, check_answer, refuse_object_scope, serialized,
        validity_label,
    },
    resolve::Located,
};

/// The arguments `set` takes: the target, `MODE VALIDITY`, the scope flags, and the root.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::retention::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from([
///         "tinio",
///         "retention",
///         "set",
///         "photos/report.txt",
///         "governance",
///         "30d",
///         "--version-id",
///         "abc",
///         "--bypass",
///     ])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// let Verb::Set(set) = args.verb else {
///     panic!("the set verb");
/// };
/// assert_eq!(set.target, "photos/report.txt");
/// assert_eq!(set.version_id.as_deref(), Some("abc"));
/// assert!(set.bypass);
/// assert!(!set.default);
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The object (`BUCKET/KEY`) or, with `--default`, the bucket.
    #[arg(value_name = "BUCKET[/KEY]")]
    pub target: String,
    /// `governance` or `compliance`.
    #[arg(value_name = "MODE", value_parser = parse_mode)]
    pub mode: model::Mode,
    /// `Nd` days or `Ny` years.
    #[arg(value_name = "VALIDITY", value_parser = parse_validity)]
    pub validity: Validity,
    /// Address one version; absent, the current version is addressed.
    #[arg(long, value_name = "ID")]
    pub version_id: Option<String>,
    /// Ask for the governance bypass (governance retentions only; never a legal hold).
    #[arg(long)]
    pub bypass: bool,
    /// Write the bucket's default-retention rule; takes a bucket, no `--version-id` and no
    /// `--bypass`.
    #[arg(long)]
    pub default: bool,
    /// Emit the subcommand's `mc` shape as JSON.
    #[command(flatten)]
    pub output: OutputArgs,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Sets the retention the flags name.
///
/// **`--default` writes the bucket configuration** through the plane's enable path — which
/// enables versioning first when the bucket is unversioned (OL24) — and carries a
/// `{mode, period}` rule. **Without it**, one object version is addressed and the plane stores
/// the computed `retain_until`; `--bypass` travels as the plane's governance authority.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a target or flag combination the contract does not
/// admit (exit 2), no running server for the root (exit 1), and the plane's refusal in its own
/// words (exit 1).
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command, Discovery, Provenance},
///     commands::retention::{self, Verb},
///     resolve::locate,
/// };
///
/// # async fn set() -> Result<(), tinio_cli::Error> {
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
///     .try_get_matches_from([
///         "tinio",
///         "retention",
///         "set",
///         "photos",
///         "--default",
///         "governance",
///         "30d",
///     ])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// assert!(matches!(args.verb, Verb::Set(_)));
/// let error = retention::run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(set()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server. Its parse is the parser's own, so the flag
/// combination it shows is the one `set` accepts.
pub async fn run(mut args: Args, located: Located) -> Result<ExitCode, Error> {
    let target = plan(&args)?;
    let dir = mem::take(&mut args.dir);
    let plane = Connection::connect(dir, located).await?;
    send(&target, &args, &plane).await.map(Outcome::finish)
}

/// The pure half: parse the target and enforce the contract's scopes **before** the server is
/// looked up, so a misuse is exit 2 even on a stopped root.
pub(crate) fn plan(args: &Args) -> Result<Target, Error> {
    let target = Target::parse(&args.target)?;
    if args.default {
        // The contract's `--default` row: a bucket scope and neither of the object-only flags.
        refuse_object_scope(&target, &args.target, "retention set --default")?;
        if args.version_id.is_some() {
            return Err(Error::Usage(
                "retention set --default takes no --version-id".to_owned(),
            ));
        }
        if args.bypass {
            return Err(Error::Usage(
                "retention set --default takes no --bypass".to_owned(),
            ));
        }
    } else {
        target.require_key("retention set", &args.target)?;
    }
    Ok(target)
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute<P: Plane>(args: &Args, plane: &P) -> Result<Outcome, Error> {
    let target = plan(args)?;
    send(&target, args, plane).await
}

async fn send<P: Plane>(target: &Target, args: &Args, plane: &P) -> Result<Outcome, Error> {
    if !args.default {
        let key = target.require_key("retention set", &args.target)?;
        let until = retain_until(args.validity)?;
        let update = set_update(
            key,
            args.version_id.as_deref(),
            args.mode,
            until,
            args.bypass,
        );
        let answer = plane
            .object_put(&target.bucket, &serialized(&args.target, &update)?)
            .await?;
        let version = args.version_id.as_deref().unwrap_or_default();
        if let Some(outcome) =
            check_answer(&args.target, &answer, 204, args.output.json, |error| {
                RetentionMessage::failure(&error, target, version)
            })?
        {
            return Ok(outcome);
        }
        let until = format_instant(until)?;
        if args.output.json {
            Outcome::json(&RetentionMessage::success(
                args.mode, &until, target, version,
            ))
        } else {
            Ok(Outcome::done(format!(
                "retention for {}: {} until {until}",
                target.urlpath(),
                args.mode
            )))
        }
    } else {
        let config = default_rule(args.mode, args.validity);
        let human = format!(
            "default retention for {}: {} {}",
            target.bucket,
            args.mode,
            validity_label(args.validity.period())
        );
        write_config(
            &args.target,
            &target.bucket,
            &config,
            "set",
            args.output.json,
            human,
            plane,
        )
        .await
    }
}
