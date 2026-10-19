//! `tinio lifecycle add` — upsert one rule of the bucket's configuration by its ID.

use std::process::ExitCode;

use http::Method;

use super::{DirArgs, Plane, refusal, require_status};
use crate::{
    _core::lifecycle::{Config, Rule},
    Error,
    resolve::Located,
};

/// The arguments `add` takes: the rule's ID and the Days-based actions it may carry.
///
/// The flags cover the Days-based actions only — a `Date`, an `ExpiredObjectDeleteMarker` or a
/// `NewerNoncurrentVersions` is [`put`](super::put)'s to express, whose body is the whole stored
/// model (`contracts/cli.md` §`tinio lifecycle`).
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
/// let matches = Cli::command()
///     .try_get_matches_from([
///         "tinio",
///         "lifecycle",
///         "add",
///         "photos",
///         "--id",
///         "logs",
///         "--prefix",
///         "logs/",
///         "--expire-days",
///         "30",
///     ])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Lifecycle(args) = cli.command else {
///     panic!("the lifecycle arm");
/// };
/// let Verb::Add(add) = args.verb else {
///     panic!("the add verb");
/// };
/// assert_eq!(add.id, "logs");
/// assert_eq!(add.prefix.as_deref(), Some("logs/"));
/// assert_eq!(add.expire_days, Some(30));
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The bucket whose configuration the rule belongs to.
    #[arg(value_name = "BUCKET")]
    pub bucket: String,
    /// The rule's identity: the rule carrying this ID is replaced, and every other rule is left
    /// alone (LC14).
    #[arg(long, value_name = "ID")]
    pub id: String,
    /// `Filter.Prefix`: the keys the rule selects. Absent, the rule selects every key.
    #[arg(long, value_name = "PREFIX")]
    pub prefix: Option<String>,
    /// `Expiration.Days`: days after creation the current version expires.
    #[arg(long, value_name = "N")]
    pub expire_days: Option<i32>,
    /// `NoncurrentVersionExpiration.NoncurrentDays`: days a version stays once it is noncurrent.
    #[arg(long, value_name = "N")]
    pub noncurrent_days: Option<i32>,
    /// `AbortIncompleteMultipartUpload.DaysAfterInitiation`: days after which an incomplete upload
    /// is aborted.
    #[arg(long, value_name = "N")]
    pub abort_days: Option<i32>,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Upserts `args`'s rule in the configuration the plane serves for `args.bucket`.
///
/// **Read, merge, write** (LC14): the stored configuration is the plane's `GET`, the one rule is
/// replaced or appended by its ID, and the merged document is the plane's `PUT` — so the other
/// rules survive an `add`, and the plane validates the document it is handed exactly as it
/// validates one a `put` sends (LC8).
///
/// # Errors
///
/// No running server for the root (exit 1, `stop`'s sentence), a stored configuration this build
/// cannot decode (exit 1), and the plane's refusal in its own words (exit 1) — a rule with no
/// action, a duplicate ID or a bad day count arrives as `400 {"rule","reason"}` and prints the
/// rule's ID with the model's reason.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::lifecycle::add::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn add() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     bucket: "photos".into(),
///     id: "logs".into(),
///     prefix: Some("logs/".into()),
///     expire_days: Some(30),
///     noncurrent_days: None,
///     abort_days: None,
///     dir: Default::default(),
/// };
/// let code = run(args, located).await?;
/// // 0 once the merged configuration is stored; a refusal is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// The example is `no_run` because it needs a running server; a live call upserts the rule into
/// the bucket's stored configuration and exits 0.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let rule = args.rule();
    let plane = Plane::connect(args.dir, located, &args.bucket).await?;
    let stored = plane.call(Method::GET, None).await?;
    let mut config = match stored.status {
        200 => stored_config(&args.bucket, &stored.body)?,
        // Nothing stored: the merge starts from an empty configuration. A `404` is also a bucket
        // that is gone and a run carrying no capability (LC15, LC23), and those are the plane's to
        // refuse — the `PUT` below answers with its own sentence rather than this command reading
        // the body of the `404` to guess which it is.
        404 => Config { rules: Vec::new() },
        _ => return Err(refusal(&args.bucket, &stored)),
    };
    config.upsert_rule(rule);
    let answer = plane.call(Method::PUT, Some(&config.to_wire())).await?;
    require_status(&args.bucket, &answer, 204)?;
    Ok(ExitCode::SUCCESS)
}

impl Args {
    /// The one rule these flags spell: `Enabled`, the prefix it names, and only the actions the
    /// flags carry.
    fn rule(&self) -> Rule {
        Rule::days(
            Some(self.id.clone()),
            self.prefix.clone(),
            self.expire_days,
            self.noncurrent_days,
            self.abort_days,
        )
    }
}

/// The configuration the plane served, decoded: the stored row and the plane's body are one shape
/// (LC17), so the model is its own decoder.
fn stored_config(bucket: &str, body: &str) -> Result<Config, Error> {
    serde_json::from_str(body).map_err(|err| {
        Error::Operational(format!(
            "lifecycle {bucket}: the stored configuration is not the model: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::lifecycle::{Expiration, Filter, Status};

    /// A rule with an ID and one `Expiration.Days`, which is all the merge compares.
    fn rule(id: &str, days: i32) -> Rule {
        Rule {
            id: Some(id.to_string()),
            status: Status::Enabled,
            filter: Filter::default(),
            expiration: Some(Expiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: None,
            }),
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: None,
        }
    }

    /// `add` is an upsert: the rule with the same ID is replaced in place and a new ID is
    /// appended, with no other rule touched (LC14).
    #[test]
    fn merge_replaces_the_rule_with_the_same_id_and_appends_a_new_one() {
        let mut config = Config {
            rules: vec![rule("logs", 30), rule("tmp", 1)],
        };

        config.upsert_rule(rule("tmp", 7));
        assert_eq!(config.rules.len(), 2, "an upsert never adds a rule");
        assert_eq!(config.rules[0].id.as_deref(), Some("logs"));
        assert_eq!(
            config.rules[1]
                .expiration
                .as_ref()
                .and_then(|expiration| expiration.days),
            Some(7),
            "the rule carrying the ID is the one replaced"
        );

        config.upsert_rule(rule("archive", 90));
        assert_eq!(config.rules.len(), 3);
        assert_eq!(config.rules[2].id.as_deref(), Some("archive"));
    }
}
