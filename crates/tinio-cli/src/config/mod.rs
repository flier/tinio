//! Configuration values the CLI parses: the root document and the machinery around it.
//!
//! The sections the CLI owns live here (`server`, `scanner`, `storage`, `api`, `s3`,
//! `telemetry`); the sections a runtime crate owns are imported from their owner —
//! `tinio_server::{log, pipeline, identity::{auth, owner, users}}` — and the root document
//! composes both halves. Discovery ([`discover`]), `.env` loading ([`sources`]), credential
//! generation ([`credentials`]), the state-directory parser ([`state`]) and the error type
//! ([`Error`]) live here too: this module is the composition root.
//!
//! Module layout: [`api`], [`s3`], [`scanner`], [`server`], [`storage`], [`telemetry`] (the
//! CLI-owned sections), [`discover`] (config discovery and the first-start auto-create),
//! [`sources`] (`.env` loading), [`credentials`] (the generated `[auth]` pair), [`state`]
//! (`TINIO_STATE_DIR` parsing) and [`Error`]. Validation is derive-driven
//! ([`garde::Validate`] on every section); [`Config::parse`] runs it after deserialization and
//! maps the report onto [`Error`].
//!
//! Section types live under their module path (`log::Config`, `api::Http`); the root document
//! is [`Config`].

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::read_to_string,
    path::Path,
};

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::{
    _core::acl::{DEFAULT_ROOT_ACCESS_KEY, OwnerId},
    _server::{
        identity::{self, auth, owner, users},
        log, pipeline,
    },
};

pub mod api;
pub mod credentials;
pub mod discover;
mod error;
pub mod s3;
pub mod scanner;
pub mod server;
pub mod sources;
pub mod state;
pub mod storage;
pub mod telemetry;

use credentials::Credentials;

pub use self::error::Error;

/// Config format version (currently only `1`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::Config;
///
/// assert!(Config::parse("version = 1").is_ok());
/// assert!(Config::parse("version = 2").is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault, Serialize, Deserialize, Validate)]
#[serde(transparent)]
pub struct Version(
    #[garde(range(min = 1, max = 1))]
    #[default = 1]
    u32,
);

/// The resolved server configuration (contracts/config.md).
///
/// Parsed from the TOML config file with fail-fast validation: unknown keys
/// and sections are rejected (collected by `serde_ignored`), value rules are
/// enforced by garde (presence-gated sections, port rules, HTTPS cert/key,
/// credential pairs, the closed access-log variable set), and violations
/// fail startup.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::Config;
///
/// let config = Config::parse(
///     r#"
///     version = 1
///
///     [server]
///     host = "0.0.0.0"
///     port = 9000
///
///     [s3]
///     multipart = false
///     "#,
/// )
/// .unwrap();
/// assert_eq!(config.server.host, "0.0.0.0");
/// assert_eq!(config.server.port, 9000);
/// assert!(!config.s3.as_ref().unwrap().capabilities.multipart);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(custom(validate_owner_and_users))]
pub struct Config {
    /// Config format version.
    #[serde(default)]
    #[garde(dive)]
    pub version: Version,
    /// The S3 data-plane listener.
    #[serde(default)]
    #[garde(dive)]
    pub server: server::Config,
    /// Background ETag scanner (presence = on, FR-024).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub scanner: Option<scanner::Config>,
    /// S3 credentials (generated on first start).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub auth: Option<auth::Config>,
    /// Logging configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub log: Option<log::Config>,
    /// S3 capability toggles (runtime level; compile-time features strip the
    /// code, FR-021).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub s3: Option<s3::Config>,
    /// Backend behavior keys (filesystem-only in v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub storage: Option<storage::Config>,
    /// The task pipelines (`[pipeline.io]` / `[pipeline.db]`; absent =
    /// defaults).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pipeline: Option<pipeline::Config>,
    /// Management-plane transports (presence-gated subsections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub api: Option<api::Config>,
    /// The root owner element (`[owner]`; absent = the core defaults
    /// `hex(SHA-256("tinio"))` / `"tinio"`, no local mapping).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub owner: Option<owner::Config>,
    /// Additional configured users (`[[users]]`; absent = root only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub users: Option<Vec<users::Config>>,
    /// OpenTelemetry export (opt-in; requires the `otel` feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub telemetry: Option<telemetry::Config>,
}

impl Config {
    /// Parse a TOML string and run fail-fast validation.
    ///
    /// This is the spelling for text with **no file behind it**: the configuration bytes a
    /// launcher forwarded to a confined child (design decision 18) parse here, because the
    /// confinement exposes no path for [`load`](Self::load) to open. Everywhere else production
    /// loads a document with `load` or builds the struct; tests and examples use this.
    pub fn parse(input: &str) -> Result<Self, Error> {
        Self::parse_at(input, Path::new("(inline)"))
    }

    /// Read, parse, and validate a config file.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = read_to_string(path).map_err(|e| error::io(path, e))?;
        Self::parse_at(&text, path)
    }

    /// Parse + validation with errors labeled by `path`.
    fn parse_at(input: &str, path: &Path) -> Result<Self, Error> {
        let document: toml::Table =
            toml::from_str(input).map_err(|e| error::parse(path, e.to_string()))?;
        let mut ignored = BTreeSet::new();
        let parsed = serde_ignored::deserialize(document.clone(), |p| {
            ignored.insert(p.to_string());
        });
        collect_flattened_unknowns(&document, &mut ignored)?;
        if !ignored.is_empty() {
            // Unknown keys were present even though deserialization might also
            // have failed (e.g. a missing required field) — report them all,
            // matching the old deny-fail-fast. The `BTreeSet` keeps the
            // iteration (and therefore the error text) deterministic.
            return Err(error::unknown_key(
                ignored
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        let config: Config = parsed.map_err(|e| error::parse(path, e.to_string()))?;
        config.validate().map_err(Error::from_report)?;
        Ok(config)
    }

    /// The backend `[storage] type` selects (PG5): the key's own default when there is no
    /// `[storage]` section at all.
    ///
    /// **This is the one reading of the selector**, so the serving half's refusal, the resolution's
    /// pg decision and the root-side diagnostics cannot disagree about which backend a document
    /// names.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::{Config, storage::Backend};
    ///
    /// assert_eq!(Config::default().backend(), Backend::Fs);
    /// let pg = Config::parse("version = 1\n[storage]\ntype = \"pg\"").expect("config");
    /// assert_eq!(pg.backend(), Backend::Pg);
    /// ```
    pub fn backend(&self) -> storage::Backend {
        self.storage
            .as_ref()
            .map_or(storage::Backend::Fs, |storage| storage.backend)
    }

    /// The `[storage.pg]` section, when the document has one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::Config;
    ///
    /// let pg = Config::parse(
    ///     "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"postgres://db/tinio\"",
    /// )
    /// .expect("config");
    /// assert_eq!(pg.pg_section().expect("the section").pool, 8);
    /// assert!(Config::default().pg_section().is_none());
    /// ```
    pub fn pg_section(&self) -> Option<&storage::pg::Config> {
        self.storage
            .as_ref()
            .and_then(|storage| storage.pg.as_ref())
    }

    /// Serialize to TOML (used by the first-start auto-create).
    ///
    /// Plain `toml` (the read side's serializer, Q8): the presence-gated
    /// serde attributes still apply, so absent sections — including
    /// `[pipeline.*]` — are never emitted. The `toml_edit` write path was
    /// an unmotivated second serializer for the same format (F47).
    pub fn to_toml(&self) -> Result<String, Error> {
        toml::to_string(self).map_err(|e| error::parse(Path::new("(serialization)"), e.to_string()))
    }

    /// The config a first start writes (FR-007, design decision 15): the
    /// defaults plus the generated credentials, with the local management
    /// channel made explicit.
    ///
    /// Every presence-gated section whose absence is the default stays out —
    /// `[log]`, `[s3]`, `[pipeline.*]`, `[owner]`, `[[users]]`, `[telemetry]`
    /// and the transports the run does not use are all `None` — so the file
    /// an operator opens is the shape `contracts/config.md` documents: the
    /// sections a run needs, and no others. The keys of the sections it does
    /// write are spelled out (the contract's example shows them uncommented),
    /// which is what makes `[server] port`/`read_only` and the `[storage.fs]`
    /// defaults editable in place. The local channel's
    /// platform form is the section `[api.unix]` (unix) or `[api.pipe]`
    /// (Windows) with an empty `path`: the derived default (decision 24),
    /// written so the channel is visible and editable rather than implied.
    /// Its `[server] read_only` records the mode the run resolved, so a
    /// read-only root keeps its mode on the next start.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::{Config, credentials};
    ///
    /// let generated = Config::generated(false, credentials::generate()?).unwrap();
    /// assert_eq!(generated.server.port, 9000);
    /// assert!(generated.auth.is_some());
    /// assert!(generated.log.is_none());
    /// # Ok::<(), tinio_cli::config::Error>(())
    /// ```
    pub fn generated(read_only: bool, credentials: Credentials) -> Result<Self, Error> {
        let config = Self {
            version: Version::default(),
            server: server::Config {
                read_only,
                ..server::Config::default()
            },
            scanner: Some(scanner::Config::default()),
            auth: Some(auth::Config {
                access_key: credentials.access_key,
                secret_key: credentials.secret_key,
            }),
            storage: Some(storage::Config::default()),
            api: Some(api::Config {
                #[cfg(unix)]
                unix: Some(api::Unix::default()),
                #[cfg(windows)]
                pipe: Some(api::Pipe::default()),
                ..api::Config::default()
            }),
            ..Self::default()
        };
        // A generated config the parser would reject is a bug, not a file:
        // validate it here, where the failure can still name its key.
        config.validate().map_err(Error::from_report)?;
        Ok(config)
    }

    /// The effective `[owner]` element: the configured section, or the
    /// core defaults when absent (spec §6).
    pub fn owner(&self) -> owner::Config {
        self.owner.clone().unwrap_or_default()
    }

    /// The configured `[[users]]` entries (empty when absent).
    pub fn users(&self) -> &[users::Config] {
        self.users.as_deref().unwrap_or_default()
    }

    /// The identity module's composed config: the three section values its functions read, with
    /// the `[owner]` default applied when the section is absent.
    ///
    /// **The composition lives here because the root document is the composition root**: an
    /// absent `[owner]` is the core default, exactly as [`Config::owner`] answers it, and the
    /// identity functions read this value rather than a second reading of the document.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::Config;
    ///
    /// let identity = Config::parse("version = 1").expect("config").identity();
    /// assert!(identity.auth.is_none());
    /// assert_eq!(identity.owner.display_name, "tinio");
    /// assert!(identity.users.is_empty());
    /// ```
    pub fn identity(&self) -> identity::Config {
        identity::Config {
            auth: self.auth.clone(),
            owner: self.owner(),
            users: self.users().to_vec(),
        }
    }
}

/// Spec §6 uniqueness rules: unique access keys and unique canonical IDs,
/// with the root `[auth]` credential and the root `[owner]` element in the
/// set — a user access key equal to the root credential would shadow it
/// (the access-key map keys by access key), and a canonical ID equal to
/// the root owner's would make the user root.
fn validate_owner_and_users(config: &Config, _context: &()) -> garde::Result {
    // The effective root access key: the `[auth]` credential, or the US1
    // fallback pair when the section is absent (DEFAULT_ROOT_ACCESS_KEY —
    // the same fallback the identity assembly uses).
    let root_key = match &config.auth {
        Some(auth) => auth.access_key.as_str(),
        None => DEFAULT_ROOT_ACCESS_KEY,
    };
    if let Some((i, user)) = config
        .users()
        .iter()
        .enumerate()
        .find(|(_, user)| user.access_key == root_key)
    {
        return Err(garde::Error::new(format!(
            "users[{i}].access_key: duplicate access key `{}` — a configured user may not shadow the root [auth] credential (or the default `{root_key}` fallback when [auth] is absent)",
            user.access_key
        )));
    }
    let mut first_index = BTreeMap::<&str, usize>::new();
    for (i, user) in config.users().iter().enumerate() {
        if let Some(first) = first_index.insert(user.access_key.as_str(), i) {
            return Err(garde::Error::new(format!(
                "users[{i}].access_key: duplicate access key `{}` (first at users[{first}])",
                user.access_key
            )));
        }
    }
    // Invalid canonical IDs (the anon ID included) are the per-field
    // rules' report — skip them here and let the dive surface the
    // path-keyed error.
    let Ok(owner_id) = OwnerId::new(config.owner().canonical_id.clone()) else {
        return Ok(());
    };
    let mut first_index = BTreeMap::<String, usize>::new();
    for (i, user) in config.users().iter().enumerate() {
        let Some(id) = user.effective_canonical_id() else {
            continue;
        };
        let id_str = id.as_str();
        if id == owner_id {
            return Err(garde::Error::new(format!(
                "users[{i}].canonical_id: duplicate canonical ID `{id_str}` — a user with the root [owner] element would become root"
            )));
        }
        if let Some(first) = first_index.insert(id_str.to_string(), i) {
            return Err(garde::Error::new(format!(
                "users[{i}].canonical_id: duplicate canonical ID `{id_str}` (first at users[{first}])"
            )));
        }
    }
    Ok(())
}

/// `#[serde(flatten)]` buffers its fields, so `serde_ignored` never sees a key inside a
/// flattened table (`[s3]`'s capability half, `[api.https]`'s `Http` half). Compare each
/// present table's keys against the owning type's serialized default — every accepted key
/// appears there — and record the extras with the path spelling `serde_ignored` uses.
type FlattenedKeySet = fn() -> Result<toml::Table, Error>;

fn collect_flattened_unknowns(
    document: &toml::Table,
    ignored: &mut BTreeSet<String>,
) -> Result<(), Error> {
    let tables: [(&[&str], FlattenedKeySet); 2] = [
        (&["s3"], || serialized_keys(&s3::Config::default())),
        (
            &["api", "https"],
            || serialized_keys(&api::Https::default()),
        ),
    ];
    for (path, keys) in tables {
        let Some(table) = table_at(document, path) else {
            continue;
        };
        let known = keys()?;
        let prefix = path.join(".");
        for key in table.keys() {
            if !known.contains_key(key) {
                ignored.insert(format!("{prefix}.{key}"));
            }
        }
    }
    Ok(())
}

/// Walk a document's table path (`["api", "https"]`), or `None` when a segment is absent or
/// not a table.
fn table_at<'a>(document: &'a toml::Table, path: &[&str]) -> Option<&'a toml::Table> {
    let mut table = document;
    for segment in path {
        table = table.get(*segment)?.as_table()?;
    }
    Some(table)
}

/// The accepted keys of a flattened table: the owning type's serialized default is flattened
/// (both halves land in one map) and carries every accepted key.
fn serialized_keys<T: Serialize>(default: &T) -> Result<toml::Table, Error> {
    match toml::Value::try_from(default) {
        Ok(toml::Value::Table(table)) => Ok(table),
        Ok(_) => Err(error::parse(
            Path::new("(serialization)"),
            "a flattened default did not serialize to a table",
        )),
        Err(e) => Err(error::parse(Path::new("(serialization)"), e.to_string())),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
