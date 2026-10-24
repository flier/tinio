//! `tinio retention` — set, read and clear a version's retention or the bucket's default rule
//! (`contracts/cli.md` §`tinio retention`).
//!
//! **The two scopes are different plane routes**: a keyed verb addresses one version through
//! `/buckets/{bucket}/object-lock/objects` (the key and version in the JSON body, OL29), while
//! `--default` reads or writes the bucket configuration through
//! `/buckets/{bucket}/object-lock`. `set --default` and `object-lock enable` go through the
//! plane's enable path, which enables versioning first when the bucket is unversioned (OL24);
//! `clear --default` never enables and writes nothing on a bucket that is not enabled.
//!
//! `MODE` is `governance` or `compliance`; `VALIDITY` is `Nd` days or `Ny` years, never an
//! absolute date (the `mc` surface's own restriction). The retain-until instant is computed here
//! with the model's calendar-year rule and sent as RFC3339.

pub mod clear;
pub mod info;
pub mod set;

use std::{process::ExitCode, time::SystemTime};

use clap::{Args as ClapArgs, Subcommand};
use serde::Serialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub use super::DirArgs;
use super::{
    object_lock::object_update,
    plane::{
        ConfigMessage, ErrorMessage, Outcome, OutputArgs, Plane, Target, check_answer, serialized,
    },
};
use crate::{
    _api::object_lock::UpdateObjectLock, _core::object_lock as model, Error, resolve::Located,
};

/// The `tinio retention` subcommand: the contract's three verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "retention", "info", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::Retention(_)));
/// ```
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The verb and its own arguments.
    #[command(subcommand)]
    pub verb: Verb,
}

/// The contract's three verbs, each with the arguments its own row lists.
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
/// fn verb(argv: &[&str]) -> Verb {
///     let matches = Cli::command().try_get_matches_from(argv).expect("parse");
///     let cli = Cli::from_arg_matches(&matches).expect("from matches");
///     let Command::Retention(args) = cli.command else {
///         panic!("the retention arm");
///     };
///     args.verb
/// }
///
/// assert!(matches!(
///     verb(&[
///         "tinio",
///         "retention",
///         "set",
///         "photos/a.txt",
///         "governance",
///         "30d"
///     ]),
///     Verb::Set(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "retention", "info", "photos"]),
///     Verb::Info(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "retention", "clear", "photos", "--default"]),
///     Verb::Clear(_)
/// ));
/// ```
#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Set a version's retention or the bucket's default rule.
    Set(set::Args),
    /// Print a version's retention or the bucket's configuration.
    Info(info::Args),
    /// Clear a version's retention or the bucket's default rule.
    Clear(clear::Args),
}

impl Args {
    /// The root, the mode and the environment channels every verb flattens.
    pub(crate) fn dir_args(&self) -> &DirArgs {
        match &self.verb {
            Verb::Set(args) => &args.dir,
            Verb::Info(args) => &args.dir,
            Verb::Clear(args) => &args.dir,
        }
    }
}

/// Runs the verb the command line named.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), the refusal a root with nothing running answers
/// (exit 1), and the plane's refusal in its own words (exit 1).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::retention::{Args, Verb, info, run},
///     resolve::locate,
/// };
///
/// # async fn show() -> Result<(), tinio_cli::Error> {
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
///     verb: Verb::Info(info::Args {
///         target: "photos".into(),
///         version_id: None,
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
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(show()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    match args.verb {
        Verb::Set(args) => set::run(args, located).await,
        Verb::Info(args) => info::run(args, located).await,
        Verb::Clear(args) => clear::run(args, located).await,
    }
}

/// A validity's unit and count: `Nd` days or `Ny` years.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::commands::retention::{Validity, parse_validity};
///
/// assert_eq!(parse_validity("30d").expect("days"), Validity::Days(30));
/// assert_eq!(parse_validity("3y").expect("years"), Validity::Years(3));
/// assert!(parse_validity("30D").is_err(), "the suffix is lowercase");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    /// A fixed span of days, `1..=36_500`.
    Days(u32),
    /// Calendar years, `1..=100`: 29 February plus one year is 28 February.
    Years(u32),
}

impl Validity {
    /// The plane's period.
    pub(crate) fn period(self) -> model::Period {
        match self {
            Self::Days(days) => model::Period::Days(days),
            Self::Years(years) => model::Period::Years(years),
        }
    }
}

/// `MODE`'s parser: `governance` or `compliance`, in any ASCII case (`mc` upper-cases the value
/// at its own parse).
pub(crate) fn parse_mode(raw: &str) -> Result<model::Mode, String> {
    if raw.eq_ignore_ascii_case("governance") {
        Ok(model::Mode::Governance)
    } else if raw.eq_ignore_ascii_case("compliance") {
        Ok(model::Mode::Compliance)
    } else {
        Err(format!(
            "mode `{raw}` is `governance` or `compliance`, not an arbitrary string"
        ))
    }
}

/// `VALIDITY`'s parser: `<N>d` or `<N>y`, the model's bounds included.
///
/// The suffix is lowercase only — the contract spells `Nd`/`Ny` — and the count is a non-negative
/// decimal integer, so `+1d`, `1.5d` and `1D` are refusals rather than values clap would quietly
/// normalize.
pub fn parse_validity(raw: &str) -> Result<Validity, String> {
    let mut chars = raw.chars();
    let unit = chars
        .next_back()
        .ok_or_else(|| "a validity is `<N>d` or `<N>y`".to_owned())?;
    let digits = chars.as_str();
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "validity `{raw}` is `<N>d` (days) or `<N>y` (years)"
        ));
    }
    let count = digits
        .parse::<u32>()
        .map_err(|_| format!("validity `{raw}` is out of range"))?;
    match unit {
        'd' if (1..=model::MAX_DAYS).contains(&count) => Ok(Validity::Days(count)),
        'y' if (1..=model::MAX_YEARS).contains(&count) => Ok(Validity::Years(count)),
        'd' => Err(format!("validity `{raw}` is 1d..={}d", model::MAX_DAYS)),
        'y' => Err(format!("validity `{raw}` is 1y..={}y", model::MAX_YEARS)),
        other => Err(format!(
            "validity `{raw}` ends in `{other}`: the units are `d` and `y`"
        )),
    }
}

/// The instant an explicit `set` protects until: `now` plus the validity, through the model's
/// calendar-year rule, so 29 February plus one year is 28 February.
pub(crate) fn retain_until(validity: Validity) -> Result<SystemTime, Error> {
    // Only the period decides the instant, so the mode is a placeholder the model still requires.
    let rule = model::Rule {
        default_retention: model::DefaultRetention {
            mode: model::Mode::Governance,
            period: validity.period(),
        },
    };
    Ok(model::default_retention_at(&rule, SystemTime::now()))
}

/// An instant as the plane's RFC3339 spelling, for the output shapes.
pub(crate) fn format_instant(instant: SystemTime) -> Result<String, Error> {
    OffsetDateTime::from(instant)
        .format(&Rfc3339)
        .map_err(|err| Error::Operational(format!("the retention date cannot be rendered: {err}")))
}

/// The retention `--json` shape (`mc`'s `{mode, until, urlpath, versionID, status, error}`).
///
/// `status` is `success`/`error`; `error` is empty on success. A cleared retention carries empty
/// `mode` and `until`, because there is no fact to render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RetentionMessage {
    pub(crate) mode: String,
    pub(crate) until: String,
    pub(crate) urlpath: String,
    #[serde(rename = "versionID")]
    pub(crate) version_id: String,
    pub(crate) status: String,
    pub(crate) error: String,
}

impl RetentionMessage {
    /// A successful rendering of the retention in force.
    pub(crate) fn success(
        mode: model::Mode,
        until: &str,
        target: &Target,
        version_id: &str,
    ) -> Self {
        Self {
            mode: mode.to_string(),
            until: until.to_owned(),
            urlpath: target.urlpath(),
            version_id: version_id.to_owned(),
            status: "success".to_owned(),
            error: String::new(),
        }
    }

    /// The shape of a read that found no retention.
    pub(crate) fn failure(error: &str, target: &Target, version_id: &str) -> Self {
        Self {
            mode: String::new(),
            until: String::new(),
            urlpath: target.urlpath(),
            version_id: version_id.to_owned(),
            status: "error".to_owned(),
            error: error.to_owned(),
        }
    }
}

/// The `enabled: true, rule: …` configuration a default rule writes.
pub(crate) fn default_rule(mode: model::Mode, validity: Validity) -> model::Config {
    model::Config {
        enabled: true,
        rule: Some(model::Rule {
            default_retention: model::DefaultRetention {
                mode,
                period: validity.period(),
            },
        }),
    }
}

/// The object update that sets a retention.
pub(crate) fn set_update(
    key: &str,
    version_id: Option<&str>,
    mode: model::Mode,
    retain_until: SystemTime,
    bypass: bool,
) -> UpdateObjectLock {
    object_update(
        key,
        version_id,
        Some(Some(model::Retention { mode, retain_until })),
        None,
        bypass,
    )
}

/// The object update that clears a retention with the implicit governance bypass (`mc` always
/// passes it on clear, OL24).
pub(crate) fn clear_update(key: &str, version_id: Option<&str>) -> UpdateObjectLock {
    object_update(key, version_id, Some(None), None, true)
}

/// Write a bucket configuration and answer the verb's output.
pub(crate) async fn write_config<P: Plane>(
    subject: &str,
    bucket: &str,
    config: &model::Config,
    op: &'static str,
    json: bool,
    human: String,
    plane: &P,
) -> Result<Outcome, Error> {
    let answer = plane
        .bucket_put(bucket, &serialized(subject, config)?)
        .await?;
    if let Some(outcome) =
        check_answer(subject, &answer, 204, json, |error| ErrorMessage { error })?
    {
        return Ok(outcome);
    }
    if json {
        Outcome::json(&ConfigMessage::from_body(op, config))
    } else {
        Ok(Outcome::done(human))
    }
}
