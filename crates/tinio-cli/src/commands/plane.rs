//! The CLI's shared management-plane vocabulary: the connection to the server that answers for a
//! root, the four-call trait the verbs' request/answer halves are generic over, the
//! `BUCKET[/KEY]` target, the printed outcome, and the wire refusal/render helpers.
//!
//! **It lives beside the groups rather than inside one of them.** `object-lock`, `retention` and
//! `legalhold` all speak these same shapes to the same plane; keeping the vocabulary in the
//! object-lock module made every retention and legal-hold verb import its neighbour's home and
//! turned that one module into two reasons to change. The Object Lock verbs, their request DTO
//! constructors and their routes stay in `super::object_lock`.

use std::process::ExitCode;

use async_trait::async_trait;
use clap::Args as ClapArgs;
use serde::{Serialize, de::DeserializeOwned};

use super::{DirArgs, server_state};
use crate::{
    _api::{
        ErrorBody, State,
        client::{self, RawResponse},
    },
    _core::{bucket, object, object_lock as model},
    Error,
    resolve::Located,
};

/// The `--json` switch every verb carries: one flag whose output shape is the subcommand's
/// (`contracts/cli.md` §`tinio retention`: the shapes mirror `mc`'s field names).
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
///     .try_get_matches_from(["tinio", "object-lock", "enable", "photos", "--json"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::ObjectLock(args) = cli.command else {
///     panic!("the object-lock arm");
/// };
/// let Verb::Enable(enable) = args.verb else {
///     panic!("the enable verb");
/// };
/// assert!(enable.output.json);
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct OutputArgs {
    /// Emit the subcommand's `mc` shape as JSON instead of the human line.
    #[arg(long)]
    pub json: bool,
}

/// What a verb prints and the code it exits with.
///
/// A refusal the plane answers is normally an [`Error`]; the two reads keep an error in the JSON
/// shape when `--json` asked for one (`retention`'s `status`/`error` fields), which is why the
/// success bit travels beside the text rather than being implied by `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Outcome {
    text: String,
    success: bool,
}

impl Outcome {
    /// A successful human line.
    pub(crate) fn done(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            success: true,
        }
    }

    /// A failed JSON shape: the document still prints, and the process exits 1.
    pub(crate) fn failed(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            success: false,
        }
    }

    /// A successful rendering of `value` in the subcommand's `mc` shape.
    pub(crate) fn json<T: Serialize>(value: &T) -> Result<Self, Error> {
        Ok(Self::done(serde_json::to_string(value).map_err(render)?))
    }

    /// A failed rendering of `value`: printed, and the process exits 1.
    pub(crate) fn json_failed<T: Serialize>(value: &T) -> Result<Self, Error> {
        Ok(Self::failed(serde_json::to_string(value).map_err(render)?))
    }

    /// Prints the outcome and answers the process's exit code.
    pub(crate) fn finish(self) -> ExitCode {
        println!("{}", self.text);
        if self.success {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    }

    /// The line or document a test reads.
    #[cfg(test)]
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Whether the outcome exits 0.
    #[cfg(test)]
    pub(crate) fn succeeded(&self) -> bool {
        self.success
    }
}

/// The rendering failure of an output shape: serializing our own structs cannot fail for any
/// value this crate builds, so it is an invariant rather than an operator condition.
fn render(err: serde_json::Error) -> Error {
    Error::Operational(format!("the object lock answer cannot be rendered: {err}"))
}

/// The connected management plane: one root resolution and one control channel, so a verb cannot
/// ask two servers.
#[derive(Debug)]
pub(crate) struct Connection {
    state: State,
}

impl Connection {
    /// Resolve the server that answers for `args`'s root, or the refusal a root with nothing
    /// running gets.
    ///
    /// The pair `status` and `stop` probe and not a third search: [`super::server_state`] resolves
    /// the root, decision 19's candidate pair, and the one a server is live in — the same helper
    /// the lifecycle group calls.
    pub(crate) async fn connect(args: DirArgs, located: Located) -> Result<Self, Error> {
        let state = server_state(args, located).await?;
        Ok(Self { state })
    }
}

/// The plane's four calls, as a trait so the verbs' request/answer logic is testable without a
/// live server.
#[async_trait]
pub(crate) trait Plane: Send + Sync {
    /// `GET /buckets/{bucket}/object-lock`.
    async fn bucket_get(&self, bucket: &str) -> Result<RawResponse, Error>;
    /// `PUT /buckets/{bucket}/object-lock`.
    async fn bucket_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error>;
    /// `GET /buckets/{bucket}/object-lock/objects`.
    async fn object_get(&self, bucket: &str, body: &str) -> Result<RawResponse, Error>;
    /// `PUT /buckets/{bucket}/object-lock/objects`.
    async fn object_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error>;
}

#[async_trait]
impl Plane for Connection {
    async fn bucket_get(&self, bucket: &str) -> Result<RawResponse, Error> {
        client::object_lock_get(&self.state, bucket)
            .await
            .map_err(Into::into)
    }

    async fn bucket_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        client::object_lock_put(&self.state, bucket, body)
            .await
            .map_err(Into::into)
    }

    async fn object_get(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        client::object_lock_object_get(&self.state, bucket, body)
            .await
            .map_err(Into::into)
    }

    async fn object_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        client::object_lock_object_put(&self.state, bucket, body)
            .await
            .map_err(Into::into)
    }
}

/// The contract's `BUCKET[/KEY]` positional.
///
/// The bucket is the contract's own name parser's input, so a name S3 cannot spell is a usage
/// error here rather than a route the plane must refuse; the key is the universal key rule's, so
/// a key the S3 surface would refuse never reaches the plane either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    /// The validated bucket name.
    pub(crate) bucket: String,
    /// The validated object key, or none for a bucket-scoped verb.
    pub(crate) key: Option<String>,
}

impl Target {
    /// Parse `BUCKET[/KEY]`.
    pub(crate) fn parse(raw: &str) -> Result<Self, Error> {
        let (bucket, key) = match raw.split_once('/') {
            Some((bucket, key)) => (bucket, Some(key)),
            None => (raw, None),
        };
        let Ok(bucket) = bucket::name(bucket.to_owned()) else {
            return Err(Error::Usage(format!("invalid bucket name: {bucket}")));
        };
        let key = match key {
            Some(key) => {
                if object::key(key.to_owned()).is_err() {
                    return Err(Error::Usage(format!("invalid object key: {key}")));
                }
                Some(key.to_owned())
            }
            None => None,
        };
        Ok(Self {
            bucket: bucket.to_string(),
            key,
        })
    }

    /// The key the verb needs, or the usage error that says what the target is missing.
    pub(crate) fn require_key(&self, verb: &str, raw: &str) -> Result<&str, Error> {
        self.key
            .as_deref()
            .ok_or_else(|| Error::Usage(format!("{verb} needs a key: {raw} is a bucket")))
    }

    /// The `urlpath` member: `bucket/key`, the contract's no-alias spelling.
    pub(crate) fn urlpath(&self) -> String {
        match &self.key {
            Some(key) => format!("{}/{}", self.bucket, key),
            None => self.bucket.clone(),
        }
    }
}

/// Serialize a request body this crate built.
pub(crate) fn serialized<T: Serialize>(subject: &str, value: &T) -> Result<String, Error> {
    serde_json::to_string(value).map_err(|err| {
        Error::Operational(format!(
            "object lock {subject}: the request cannot be rendered: {err}"
        ))
    })
}

/// Decode the plane's answer as the model it is.
pub(crate) fn decoded<T: DeserializeOwned>(subject: &str, raw: &RawResponse) -> Result<T, Error> {
    serde_json::from_str(&raw.body).map_err(|err| {
        Error::Operational(format!(
            "object lock {subject}: the plane's answer is not the model: {err}"
        ))
    })
}

/// The plane's `{"error": …}` body, when it carried one.
pub(crate) fn error_message(raw: &RawResponse) -> Option<String> {
    serde_json::from_str::<ErrorBody>(&raw.body)
        .ok()
        .map(|body| body.error)
}

/// The refusal sentence a raw answer carries: its `{"error": …}` body, or the body itself.
pub(crate) fn refusal_message(raw: &RawResponse) -> String {
    error_message(raw).unwrap_or_else(|| raw.body.trim().to_owned())
}

/// The `--json` error document for a verb whose `mc` success shape has no `status`/`error` field
/// (the bucket configuration's `op`/`enabled`/`mode`/`validity`, the legal hold's
/// `legalhold`/`urlpath`/`key`/`versionID`): `mc`'s one-field error stream, `{"error": "…"}`.
///
/// The verbs that do carry `error` in their success shape (the keyed retention verbs) render
/// their own shape instead; [`json_refusal`] takes whichever the caller builds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ErrorMessage {
    pub(crate) error: String,
}

/// A non-expected plane answer under `--json` as `shape`'s document, printed and exit 1; `None`
/// when the answer is the expected one or `--json` was not asked for (the caller's
/// [`require_status`] then raises the operational error).
pub(crate) fn json_refusal<T: Serialize>(
    json: bool,
    expected: u16,
    raw: &RawResponse,
    shape: impl FnOnce(String) -> T,
) -> Result<Option<Outcome>, Error> {
    if raw.status != expected && json {
        return Outcome::json_failed(&shape(refusal_message(raw))).map(Some);
    }
    Ok(None)
}

/// The status a verb requires, or the plane's own refusal sentence.
pub(crate) fn require_status(subject: &str, raw: &RawResponse, status: u16) -> Result<(), Error> {
    if raw.status != status {
        return Err(refusal(subject, raw));
    }
    Ok(())
}

/// The answer a verb gives a plane response that does not carry the `expected` status: the
/// `--json` refusal document `shape` renders when `--json` asked for one, or the plane's own
/// refusal sentence otherwise — and `None` when the response is the one the verb requires.
///
/// The pair of steps every plane verb runs after its call ([`json_refusal`] then
/// [`require_status`]) in one place, so a verb cannot reach its success shape without the status
/// check. `subject` is the target the refusal sentence names, exactly as [`require_status`]
/// spells it.
pub(crate) fn check_answer<T: Serialize>(
    subject: &str,
    raw: &RawResponse,
    expected: u16,
    json: bool,
    shape: impl FnOnce(String) -> T,
) -> Result<Option<Outcome>, Error> {
    if let Some(outcome) = json_refusal(json, expected, raw, shape)? {
        return Ok(Some(outcome));
    }
    require_status(subject, raw, expected)?;
    Ok(None)
}

/// The plane's refusal in its own words — `no such bucket`, `access denied`, `invalid bucket
/// state`, `object lock is not served here` — never a re-decision of what the status means.
pub(crate) fn refusal(subject: &str, raw: &RawResponse) -> Error {
    Error::Operational(format!("object lock {subject}: {}", refusal_message(raw)))
}

/// The bucket-configuration `--json` shape (`mc`'s `{op, enabled, mode, validity}`).
///
/// `validity` is the contract's `30DAYS`/`3YEARS` spelling, not `30d`; a bucket with no rule
/// carries empty `mode` and `validity` strings, which is how `mc` renders a bare enable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ConfigMessage {
    pub(crate) op: &'static str,
    pub(crate) enabled: bool,
    pub(crate) mode: String,
    pub(crate) validity: String,
}

impl ConfigMessage {
    /// The shape of a configuration the plane served or this run wrote.
    pub(crate) fn from_body(op: &'static str, config: &model::Config) -> Self {
        let (mode, validity) = match &config.rule {
            Some(rule) => (
                rule.default_retention.mode.to_string(),
                validity_label(rule.default_retention.period),
            ),
            None => (String::new(), String::new()),
        };
        Self {
            op,
            enabled: config.enabled,
            mode,
            validity,
        }
    }

    /// The shape of a bucket that is not enabled: `enabled:false` and no rule.
    pub(crate) fn disabled(op: &'static str) -> Self {
        Self {
            op,
            enabled: false,
            mode: String::new(),
            validity: String::new(),
        }
    }
}

/// The contract's validity spelling: `30DAYS` or `3YEARS`.
pub(crate) fn validity_label(period: model::Period) -> String {
    match period {
        model::Period::Days(days) => format!("{days}DAYS"),
        model::Period::Years(years) => format!("{years}YEARS"),
    }
}

/// A bucket-scope verb's target check: `target` must name a bucket, not an object, or the usage
/// error names what the verb took. The one spelling the retention `--default` verbs and
/// `object-lock enable` share.
pub(crate) fn refuse_object_scope(target: &Target, raw: &str, verb: &str) -> Result<(), Error> {
    if target.key.is_some() {
        return Err(Error::Usage(format!(
            "{verb} takes a bucket, not an object: {raw}"
        )));
    }
    Ok(())
}

/// The `not enabled` no-op the bucket-scope reads share: the plane's `404 "no object lock
/// configuration"` is a successful read of "disabled", never a refusal (OL24).
pub(crate) fn not_enabled(json: bool, op: &'static str, bucket: &str) -> Result<Outcome, Error> {
    if json {
        Outcome::json(&ConfigMessage::disabled(op))
    } else {
        Ok(Outcome::done(format!(
            "object lock is not enabled for {bucket}"
        )))
    }
}
