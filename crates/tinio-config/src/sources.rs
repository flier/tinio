//! Configuration source loading (task T018).
//!
//! The precedence chain `CLI flags > process env > .env > config file`
//! (FR-016) is resolved as follows:
//!
//! - [`load_env_file`] loads `<state-dir>/.env` via `dotenvy`, which does NOT
//!   override already-set process variables — process env naturally wins over
//!   `.env`;
//! - environment overlays are declared as clap `env` attributes on the CLI
//!   argument definitions (after `dotenvy::dotenv()`), so
//!   flags > env > `.env` falls out of clap's own precedence — apart from
//!   the `TINIO_READ_ONLY` overlay below, there is no manual env overlay
//!   here;
//! - the config file is the lowest-precedence source: parsed by
//!   [`crate::schema::Config::parse`] as the base, overlaid by the CLI and,
//!   in the interim ahead of the clap `env` attributes the CLI will bring
//!   (T066/T067), by the `TINIO_READ_ONLY` overlay this module resolves
//!   (FR-023).
//!
//! The `MINIO_*` credential fallback (T084) and `TINIO_ANONYMOUS` (T087)
//! extend this module in US3.

use std::{env, path::Path};

use crate::{Error, error};

/// Load `<state-dir>/.env` if present (FR-016; loaded from the state dir).
///
/// Existing process variables are never overridden (dotenvy semantics), so
/// `.env` sits below the process environment in the precedence chain.
///
/// # Examples
///
/// ```rust
/// use std::env::temp_dir;
///
/// use tinio_config::sources::load_env_file;
///
/// let dir = temp_dir();
/// // No .env in the temp dir — the call must succeed silently.
/// assert!(load_env_file(&dir).is_ok());
/// ```
pub fn load_env_file(state_dir: &Path) -> Result<(), Error> {
    let path = state_dir.join(".env");
    if path.exists() {
        dotenvy::from_path(&path).map_err(|e| error::env(path, e))?;
    }
    Ok(())
}

/// The environment variable that selects read-only mode (FR-023).
pub const READ_ONLY_VAR: &str = "TINIO_READ_ONLY";

/// Parse a `TINIO_READ_ONLY` value: `1`/`true` and `0`/`false` only.
/// Anything else is a startup error — a typo must not silently serve a
/// writable root. Deliberately stricter than `TINIO_SCANNER`, whose
/// handler (`tinio-fs`'s `scanner_enabled`) ignores an unrecognised value
/// and falls back to the `[scanner]` section gate.
pub fn parse_read_only(raw: &str) -> Result<bool, Error> {
    match raw.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(Error::invalid_value(
            READ_ONLY_VAR,
            format!("expected 0/1/true/false, got `{other}`"),
        )),
    }
}

/// The `TINIO_READ_ONLY` overlay (FR-023): `Some(_)` when the variable is
/// set, `None` when it is not (the caller keeps the config file's value),
/// and an error when it is set to something that does not parse.
pub fn env_read_only() -> Result<Option<bool>, Error> {
    parse_var(READ_ONLY_VAR, parse_read_only)
}

/// Look up `var` and parse it: `Ok(None)` when it is unset (the caller keeps
/// its own default), `Ok(Some(_))` when it is set, and an error when the
/// value is not Unicode or when `parse` rejects it. Only the not-Unicode
/// arm names `var`; a rejection carries whatever name `parse` put in its own
/// error.
///
/// Its two callers — [`env_read_only`] here and
/// [`crate::state::env_state_dir`] — need the same three cases and the same
/// error text, so the scaffold lives here once and each supplies only its
/// parse.
pub(crate) fn parse_var<T>(
    var: &str,
    parse: impl FnOnce(&str) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    from_var(var, env::var(var), parse)
}

/// [`parse_var`]'s three cases with the lookup already done.
///
/// Split out because the lookup is the only part a test cannot drive:
/// `unsafe_code = "forbid"` is per-crate (`docs/cargo.md`), so
/// `env::set_var` — `unsafe` under edition 2024 — is out of reach for every
/// test in the workspace, and the harness runs tests on parallel threads
/// over one process-global environment anyway.
fn from_var<T>(
    var: &str,
    value: Result<String, env::VarError>,
    parse: impl FnOnce(&str) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    match value {
        Ok(raw) => parse(&raw).map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(Error::invalid_value(var, "value is not valid Unicode"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, ffi::OsString, fs};

    use super::*;

    #[test]
    fn load_env_file_reads_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        // Unique name: a lingering value from a previous failed run would
        // only fail this test's assertion, never affect other tests.
        let var = "TINIO_TEST_ENV_FILE_LOADED";
        fs::write(dir.path().join(".env"), format!("{var}=yes\n")).unwrap();
        load_env_file(dir.path()).unwrap();
        assert_eq!(env::var(var).unwrap(), "yes");
    }

    #[test]
    fn load_env_file_absent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_env_file(dir.path()).is_ok());
    }

    #[test]
    fn load_env_file_refuses_a_malformed_file() {
        // Probed, not guessed: dotenvy answers `LineParse` for a line with no
        // `=`. The refusal must reach the caller as `Error::Env` naming the
        // file — FR-016 is fail-fast, and a silently ignored `.env` typo is
        // exactly the failure the strict reading prevents.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        fs::write(&path, "FOO\n").unwrap();
        let err = load_env_file(dir.path()).unwrap_err();
        assert!(
            matches!(&err, Error::Env { path: p, .. } if p == &path),
            "a refused `.env` must fail as `Error::Env` naming `{}`, got {err:?}",
            path.display()
        );
        assert!(
            matches!(
                &err,
                Error::Env {
                    source: dotenvy::Error::LineParse(..),
                    ..
                }
            ),
            "the dotenvy error itself must be carried, not replaced, got {err:?}"
        );
    }

    #[test]
    fn read_only_flag_parses_strictly() {
        // The trimmed form parses like its bare value.
        for raw in ["1", "true", " 1 "] {
            assert!(parse_read_only(raw).unwrap(), "{raw}");
        }
        for raw in ["0", "false"] {
            assert!(!parse_read_only(raw).unwrap(), "{raw}");
        }
        // An empty value is an error, never "unset" or "false".
        for raw in ["", "yes"] {
            let err = parse_read_only(raw).unwrap_err();
            assert!(err.to_string().contains(READ_ONLY_VAR), "{raw}: {err}");
        }
    }

    /// The variable the probe cases carry. It is never in the process
    /// environment — the lookup is exactly what [`from_var`] splits away —
    /// so any name works.
    const PROBE_VAR: &str = "TINIO_TEST_SOURCES_PROBE";

    /// [`parse_var`]'s three cases, driven through the value the lookup would
    /// have produced: the split that keeps an unset variable from overriding
    /// a config file, and a typo from silently serving the default.
    #[test]
    fn parse_var_distinguishes_unset_set_and_not_unicode() {
        assert_eq!(
            from_var(PROBE_VAR, Err(env::VarError::NotPresent), parse_read_only).unwrap(),
            None,
            "an unset variable is `Ok(None)` — the caller keeps its default"
        );
        assert_eq!(
            from_var(PROBE_VAR, Ok("true".into()), parse_read_only).unwrap(),
            Some(true),
            "a set variable is `Ok(Some(parsed))`"
        );
        let err = from_var(
            PROBE_VAR,
            Err(env::VarError::NotUnicode(OsString::new())),
            parse_read_only,
        )
        .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidValue { key, .. } if key == PROBE_VAR),
            "a value that is not Unicode must be `Error::InvalidValue` naming \
             `{PROBE_VAR}`, got {err:?}"
        );
    }

    /// A parse that rejects its value reaches the caller unchanged: the
    /// variant, the name and the message are the parse's own — here
    /// `parse_read_only` names `{READ_ONLY_VAR}` — not `parse_var`'s `var`
    /// argument, which only the not-Unicode arm uses.
    #[test]
    fn parse_var_leaves_the_parsers_own_error_alone() {
        let err = from_var(PROBE_VAR, Ok("yes".into()), parse_read_only).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidValue { key, .. } if key == READ_ONLY_VAR),
            "a rejected value must arrive as the parse's own `InvalidValue`, naming \
             `{READ_ONLY_VAR}` rather than the `{PROBE_VAR}` it was looked up under, got {err:?}"
        );
        assert!(
            err.to_string().contains("expected 0/1/true/false"),
            "the parse's own message must survive unchanged, got {err}"
        );
    }
}
