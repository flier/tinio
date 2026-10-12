//! The pg invocation path: the synthetic root, the effective dsn and the `DIR` refusal.
//!
//! A pg run has no served root — the database is the store — so the resolution synthesizes one
//! instead (PG5): `<home>/.tinio/pg/<sha256(effective dsn)16>/`, or `TINIO_STATE_DIR` verbatim
//! when the operator set it. Everything downstream of the root (config discovery, the state
//! directory, the control channel, the state file, the logs) is the ordinary machinery, which is
//! what makes a pg run invocable at all.
//!
//! **`[storage] type` is the selector and it is read from the configuration**, so a pg run's
//! configuration is the anchor: `--config` names it without needing a root (the one command
//! shape whose config read does not depend on the root), while a positional `DIR` or a
//! discovered `.tinio/` root is refused with the remedy (PG13) rather than ignored.

use std::path::{Path, PathBuf};

use secrecy::ExposeSecret;
use sha2::{Digest as _, Sha256};

use crate::{
    _config::{Error as ConfigError, storage::pg},
    _core::object::RESERVED_SEGMENT,
    _util::secret::Secret,
    Error,
};

/// Length of the derived root's leaf, in hexadecimal characters — the same width the
/// home-derived state directory uses ([`crate::resolve::resolve_state_dir`]'s
/// `~/.tinio/roots/<hash>/`), so the two layouts read alike.
const HASH_LEN: usize = 16;

/// The directory the tool's own state lives under for a pg run (`~/.tinio/pg/`).
const PG_DIR_NAME: &str = "pg";

/// The synthetic root a pg run resolves its configuration and tool state under (PG5).
///
/// A pg run serves no directory — the database is the store — so the root the rest of the chain
/// needs (config discovery, the state directory, the control channel, the state file, the logs)
/// is synthesized from the dsn: `<home>/.tinio/pg/<sha256(dsn)16>/` ([`synthetic_root`]), or the
/// operator's `TINIO_STATE_DIR` verbatim when it is set. Two databases therefore get two roots,
/// and the second is derived from the connection string the run actually opens — which is why
/// this takes the **effective** dsn, the `TINIO_PG_DSN` answer included (PG5).
///
/// **The root is computed, not canonicalized and not created**: it need not exist yet (the
/// serving half creates it with the state directory), and an override is the operator's
/// spelling rather than a path to resolve.
///
/// # Errors
///
/// A home directory that cannot be resolved, with the remedy named: the derivation is the only
/// thing that needs one, and `TINIO_STATE_DIR` is the escape hatch (the same pair
/// `~/.tinio/roots/<hash>/` documents for a service account with no home). An override answers
/// before the lookup, so a host without a home serves pg at the location the operator named.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::pg::resolve_root;
/// use tinio_config::auth::{Secret, SecretKey};
///
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// // An explicit location answers verbatim: the operator chose it.
/// let root = resolve_root(&dsn, Some(Path::new("/srv/tinio-pg"))).expect("an override");
/// assert_eq!(root, Path::new("/srv/tinio-pg"));
/// ```
pub fn resolve_root(dsn: &Secret, override_dir: Option<&Path>) -> Result<PathBuf, Error> {
    if let Some(dir) = override_dir {
        return Ok(dir.to_path_buf());
    }
    let home = dirs::home_dir().ok_or_else(|| {
        Error::Config(ConfigError::Missing(
            "home directory (the pg run's synthetic root is derived under it; set TINIO_STATE_DIR \
             to name the location instead)"
                .into(),
        ))
    })?;
    Ok(synthetic_root(dsn, &home))
}

/// [`resolve_root`]'s derivation with the home as an **explicit parameter**, never
/// `dirs::home_dir()` inline: a test must resolve a scratch home instead of the developer's, and
/// the caller that *wants* the environment's answer is [`resolve_root`] alone (the shape
/// `tinio_config::state::home_state_dir` uses for the read-only leaf).
///
/// The leaf is the first sixteen hex characters of the SHA-256 over the dsn's material —
/// `contracts/config.md`'s `<sha256(dsn)16>` — and nothing else is hashed, so the credential
/// itself never reaches a path.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::pg::synthetic_root;
/// use tinio_config::auth::{Secret, SecretKey};
///
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:hunter2@db.example:5432/tinio".to_owned(),
/// ));
/// let root = synthetic_root(&dsn, Path::new("/home/svc"));
/// assert_eq!(root, Path::new("/home/svc/.tinio/pg/671346790abec55e"));
/// ```
pub fn synthetic_root(dsn: &Secret, home: &Path) -> PathBuf {
    home.join(RESERVED_SEGMENT)
        .join(PG_DIR_NAME)
        .join(dsn_hash(dsn))
}

/// The leaf name: the first [`HASH_LEN`] hex characters of the SHA-256 over the dsn's material.
fn dsn_hash(dsn: &Secret) -> String {
    let digest = Sha256::digest(dsn.expose_secret().as_bytes());
    hex::encode(digest)[..HASH_LEN].to_string()
}

/// The dsn a pg run opens: the parsed `TINIO_PG_DSN` answer when there is one, else
/// `[storage.pg] dsn` (PG5).
///
/// **The environment variable is an override and never a selector**: the pg backend is chosen by
/// `[storage] type` alone, so a host that merely has the variable exported keeps serving a
/// filesystem root until the key says `pg`. The value arrives here already parsed (the CLI's
/// hidden `--pg-dsn`, whose parser is the variable's channel), never read from the environment
/// beside it — the contract's one-owner rule for `TINIO_*`.
///
/// Either source answering with an **empty** value answers nothing: `named` is that rule, and
/// both sources go through it.
///
/// # Errors
///
/// Neither source answering: a startup error naming **both** remedies, because either one is
/// what the operator has to go and set.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::resolve::pg::resolve_dsn;
/// use tinio_config::auth::{Secret, SecretKey};
///
/// let from_env = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// // The variable's answer beats the section's, and an empty section is no answer.
/// let dsn = resolve_dsn(None, Some(&from_env)).expect("the variable answered");
/// assert_eq!(dsn, from_env);
/// assert!(resolve_dsn(None, None).is_err());
/// ```
pub fn resolve_dsn(
    section: Option<&pg::Config>,
    from_env: Option<&Secret>,
) -> Result<Secret, Error> {
    let answer = named(from_env.cloned())
        .or_else(|| named(section.and_then(|pg| pg.dsn.clone())))
        .ok_or_else(|| {
            Error::Config(ConfigError::invalid_value(
                "storage.pg.dsn",
                "a pg run needs a dsn: set `[storage.pg] dsn` in the configuration file, or \
                 export TINIO_PG_DSN",
            ))
        })?;
    Ok(answer)
}

/// The dsn a source names: `None` when the source is absent **or empty**, because an empty dsn
/// names nothing. **This is the one reading of that rule**, shared by both of [`resolve_dsn`]'s
/// sources and by the readers that ask a `TINIO_PG_DSN` answer whether it named anything at all —
/// the [`Discovery`](crate::cli::Discovery) the resolution threads and the doctor's pg leg — so
/// `TINIO_PG_DSN=` and an unset variable are one answer everywhere. An exported-but-empty
/// variable therefore neither selects a pg run, nor moves a run's root to the synthetic one, nor
/// refuses a run that named no `DIR`.
pub(crate) fn named(dsn: Option<Secret>) -> Option<Secret> {
    dsn.filter(|dsn| !dsn.expose_secret().is_empty())
}

/// PG13's refusal for a **positional `DIR`** under `[storage] type = "pg"`.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::{Error, resolve::pg::refuse_positional_dir};
///
/// let refusal = refuse_positional_dir(Path::new("/srv/data")).to_string();
/// assert!(refusal.contains("--config"), "{refusal}");
/// assert!(matches!(
///     refuse_positional_dir(Path::new("/srv/data")),
///     Error::Usage(_)
/// ));
/// ```
pub fn refuse_positional_dir(dir: &Path) -> Error {
    refuse(&format!("the positional DIR `{}`", dir.display()))
}

/// PG13's refusal for the **discovered root** (the walk-up's `.tinio/` answer) under
/// `[storage] type = "pg"`.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::{Error, resolve::pg::refuse_discovered_root};
///
/// let refusal = refuse_discovered_root(Path::new("/srv/data")).to_string();
/// assert!(refusal.contains("--config"), "{refusal}");
/// assert!(matches!(
///     refuse_discovered_root(Path::new("/srv/data")),
///     Error::Usage(_)
/// ));
/// ```
pub fn refuse_discovered_root(root: &Path) -> Error {
    refuse(&format!("the discovered root `{}`", root.display()))
}

/// The one sentence both PG13 refusals render: the subject, the rule and the remedy.
fn refuse(subject: &str) -> Error {
    Error::Usage(format!(
        "`[storage] type = \"pg\"` has no served root, so {subject} would be ignored and is \
         refused: name the configuration with `--config PATH` — the one document a pg run reads \
         without a root — and drop any positional DIR; a pg run's root is synthesized from its \
         dsn (`~/.tinio/pg/<sha256(dsn)>/`, or TINIO_STATE_DIR when set)"
    ))
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs};

    use secrecy::ExposeSecret;

    use super::*;
    use crate::{
        _config::{Config, storage::Backend},
        _util::secret::SecretKey,
        cli::{Discovery, PG_DSN_VAR, Provenance},
        resolve::{locate, locate_from},
    };

    /// A `Secret` from a fixture string (`Secret` itself takes `SecretKey`, not `String`).
    fn secret(raw: &str) -> Secret {
        Secret::from(SecretKey::from(raw.to_owned()))
    }

    /// A pg configuration file on disk: the selector, and the dsn the section carries.
    fn pg_config_file(dir: &Path, dsn: Option<&str>) -> PathBuf {
        let dsn = dsn.map_or(String::new(), |dsn| format!("dsn = \"{dsn}\"\n"));
        let path = dir.join("pg.toml");
        fs::write(
            &path,
            format!("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\n{dsn}"),
        )
        .expect("write");
        path
    }

    /// The discovery answers a pg run's leg uses: the configuration, no `DIR`, nothing else.
    fn discovery_for(config: PathBuf) -> Discovery {
        Discovery {
            dir: None,
            config: Some(config),
            read_only: false,
            state_dir: None,
            pg_dsn: None,
        }
    }

    /// The discovery answers the walk-up's leg uses: no `DIR` and no `--config`, so the root can
    /// only be the ancestor walk's answer.
    fn walk_up_discovery() -> Discovery {
        Discovery {
            dir: None,
            config: None,
            read_only: false,
            state_dir: None,
            pg_dsn: None,
        }
    }

    const DSN: &str = "postgres://user:hunter2@db.example:5432/tinio";

    /// `sha256(DSN)`'s first 16 hex characters — the leaf `contracts/config.md`'s
    /// `<sha256(dsn)16>` documents. Pinned as a literal so a change of hash **input** (the dsn's
    /// text replaced by, say, its length or a parsed host) cannot pass while the shape assertions
    /// below still hold.
    const DSN_LEAF: &str = "671346790abec55e";

    #[test]
    fn the_synthetic_root_is_the_documented_hash_of_the_dsn() {
        // The derivation against a **scratch home**, so the assertion is about the hash and the
        // layout rather than about the developer's home (`synthetic_root`'s own reason).
        let home = tempfile::TempDir::new().expect("home");
        let root = synthetic_root(&secret(DSN), home.path());
        assert_eq!(root, home.path().join(".tinio").join("pg").join(DSN_LEAF));
        let leaf = root
            .file_name()
            .expect("a leaf")
            .to_string_lossy()
            .into_owned();
        assert_eq!(leaf.len(), 16, "{root:?}");
        assert!(leaf.chars().all(|c| c.is_ascii_hexdigit()), "{root:?}");
        // Deterministic: the same dsn derives the same root, a different one another.
        assert_eq!(root, synthetic_root(&secret(DSN), home.path()));
        let other = synthetic_root(&secret(&DSN.replace("tinio", "other")), home.path());
        assert_ne!(root, other);
    }

    #[test]
    fn the_derived_root_keeps_the_layout_under_the_real_home() {
        // `resolve_root`'s home branch (the environment's answer, not a scratch one): the shape
        // above, reached through the wrapper the resolution actually calls.
        let root = resolve_root(&secret(DSN), None).expect("the home resolves");
        assert_eq!(root.parent().and_then(Path::file_name), Some("pg".as_ref()));
        assert_eq!(
            root.parent()
                .and_then(Path::parent)
                .and_then(Path::file_name),
            Some(".tinio".as_ref())
        );
        assert_eq!(root.file_name(), Some(OsStr::new(DSN_LEAF)));
    }

    #[test]
    fn an_explicit_state_dir_is_the_root_verbatim() {
        // PG5: `TINIO_STATE_DIR` overrides the derivation. Taken verbatim: the operator chose
        // the location, and the directory need not exist for the resolution to name it.
        let dsn = secret(DSN);
        let chosen = PathBuf::from("/srv/tinio-pg-state");
        assert_eq!(
            resolve_root(&dsn, Some(&chosen)).expect("the override answers"),
            chosen
        );
    }

    #[test]
    fn the_environment_overrides_the_sections_dsn() {
        let section: pg::Config = {
            let config = Config::parse(&format!(
                "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"{DSN}\""
            ))
            .expect("config");
            config.storage.expect("section").pg.expect("pg")
        };
        // The env answer wins, verbatim.
        let over = secret("postgres://user:envpw@db.example:5432/other");
        let resolved = resolve_dsn(Some(&section), Some(&over)).expect("the override answers");
        assert_eq!(
            resolved.expose_secret().as_ref(),
            over.expose_secret().as_ref()
        );
        // Without it, the section's own dsn is the answer.
        let resolved = resolve_dsn(Some(&section), None).expect("the section answers");
        assert_eq!(resolved.expose_secret().as_ref(), DSN);
    }

    #[test]
    fn a_missing_dsn_is_refused_with_both_remedies() {
        // Neither source: the refusal names the key and the variable, because either one is
        // what the operator has to go and set.
        for section in [None, Some(pg::Config::default())] {
            let err = resolve_dsn(section.as_ref(), None).expect_err("no dsn");
            let rendered = err.to_string();
            assert!(rendered.contains("TINIO_PG_DSN"), "{rendered}");
            assert!(rendered.contains("dsn"), "{rendered}");
            assert!(
                matches!(err, Error::Config(ConfigError::InvalidValue { .. })),
                "{err}"
            );
        }
        // And an empty section cannot answer either.
        let empty = pg::Config {
            dsn: Some(secret("")),
            ..pg::Config::default()
        };
        assert!(resolve_dsn(Some(&empty), None).is_err());
    }

    #[test]
    fn the_pg_keys_parse_in_a_build_without_the_pg_backend() {
        // §6: the keys are not feature-gated. This crate's default feature set carries no pg
        // backend, so this leg is the feature-off leg, and the configuration parses anyway --
        // including the selector.
        let config = Config::parse(&format!(
            "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"{DSN}\"\npool = 3"
        ))
        .expect("the keys parse in every build");
        let storage = config.storage.expect("section");
        assert_eq!(storage.backend, Backend::Pg);
        assert_eq!(storage.pg.expect("pg").pool, 3);
    }

    #[test]
    fn a_positional_dir_is_refused_under_pg() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let mut discovery = discovery_for(config);
        discovery.dir = Some(dir.path().to_path_buf());
        let err = locate(discovery, &Provenance::default()).expect_err("a DIR is refused");
        assert!(matches!(err, Error::Usage(_)), "exit 2: {err}");
        let rendered = err.to_string();
        assert!(rendered.contains("--config"), "the remedy: {rendered}");
        assert!(rendered.contains("pg"), "{rendered}");
    }

    #[test]
    fn a_discovered_root_is_refused_under_pg() {
        // The walk-up's arm of PG13: a root is *found* (here by naming the starting directory —
        // no test may mutate the process's working directory, which would race every other test
        // in this binary) and the document under it selects pg, so `locate` refuses with the
        // remedy. The discovery carries no positional `DIR` and no `--config`: the root is the
        // walk-up's answer, which is the shape this arm exists for.
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("data");
        fs::create_dir_all(root.join(".tinio")).expect("root");
        fs::write(
            root.join(".tinio/.tinio.toml"),
            "version = 1\n[storage]\ntype = \"pg\"\n",
        )
        .expect("config");
        let err = locate_from(walk_up_discovery(), &Provenance::default(), &root)
            .expect_err("a discovered root is refused");
        assert!(matches!(err, Error::Usage(_)), "exit 2: {err}");
        let rendered = err.to_string();
        assert!(rendered.contains("--config"), "the remedy: {rendered}");
        assert!(rendered.contains("data"), "the subject: {rendered}");
    }

    #[test]
    fn a_pg_run_resolves_the_synthetic_root_and_keeps_the_machinery() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let located = locate(discovery_for(config), &Provenance::default()).expect("located");
        // The root is synthesized from the dsn (no DIR, no walk-up: the current directory is
        // irrelevant to this leg).
        let expected = resolve_root(&secret(DSN), None).expect("root");
        assert_eq!(located.root, expected);
        // Everything downstream is the ordinary machinery: the state directory is the root's
        // own `.tinio` (PG's read-only mode has no served root to relocate away from).
        assert_eq!(located.state_dir, expected.join(".tinio"));
    }

    #[test]
    fn an_explicit_state_dir_moves_the_synthetic_root() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let state = dir.path().join("state");
        let mut discovery = discovery_for(config);
        discovery.state_dir = Some(state.clone());
        let located = locate(discovery, &Provenance::default()).expect("located");
        assert_eq!(located.root, state);
        assert_eq!(located.state_dir, state.join(".tinio"));
    }

    #[test]
    fn an_fs_config_still_resolves_its_root_from_the_directory() {
        // The selector's default arm, through the same `locate`: a configuration that never
        // mentions pg keeps the positional root and the reserved state directory.
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("data");
        fs::create_dir_all(root.join(".tinio")).expect("root");
        fs::write(root.join(".tinio/.tinio.toml"), "version = 1").expect("config");
        let mut discovery = discovery_for(root.join(".tinio/.tinio.toml"));
        discovery.dir = Some(root.clone());
        let located = locate(discovery, &Provenance::default()).expect("located");
        assert_eq!(located.root, root.canonicalize().expect("canonical"));
        assert_eq!(
            located.state_dir,
            root.canonicalize().expect("canonical").join(".tinio")
        );
    }

    #[test]
    fn the_dsn_variable_name_is_the_contracts() {
        assert_eq!(PG_DSN_VAR, "TINIO_PG_DSN");
    }
}
