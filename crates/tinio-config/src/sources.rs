//! Configuration source loading (task T018).
//!
//! This module owns the **file** level of FR-016's precedence chain and nothing else:
//! `CLI flags > process environment > .env > config file`.
//!
//! - [`load_env_file`] loads `<state-dir>/.env` via `dotenvy`, which does NOT override
//!   already-set process variables — process env naturally wins over `.env`, and the file's
//!   own values land in the process environment, which is where the CLI's parser reads them;
//! - the environment itself is the CLI's: every variable is a clap argument with an `env`
//!   attribute (`crates/tinio-cli/src/cli.rs`), so flags, the process environment and `.env`
//!   are one parser's answers and one precedence, expressed in one place rather than
//!   re-implemented beside it. The parse is two-pass for exactly that reason — `.env` is
//!   loaded between the passes, and the second one sees the file;
//! - [`parse_read_only`] and [`parse_sandbox`] are **pure syntax helpers**: they are
//!   `tinio_util::switch::parse_switch` — the one definer of the tolerant `1`/`true`/`0`/`false`
//!   grammar, shared with the CLI's `--read-only`/`--sandbox` flags and their `value_parser`
//!   (`crates/tinio-cli/src/commands/mod.rs`) — each naming the variable whose refusal it wraps
//!   ([`READ_ONLY_VAR`], [`SANDBOX_VAR`]). No code path here reads an environment variable, and
//!   nothing in this workspace calls these two helpers: the variable names travel beside the
//!   flags that do read them;
//! - the config file is the lowest-precedence source: parsed by
//!   [`crate::schema::Config::parse`] as the base, overlaid by the CLI's resolution
//!   (`tinio-cli`'s `resolve` module).
//!
//! The `MINIO_*` credential fallback (T084) and `TINIO_ANONYMOUS` (T087) extend this module in
//! US3.

use std::path::Path;

use crate::{_util::switch::parse_switch, Error, error};

/// Load `<state-dir>/.env` if present (FR-016; loaded from the state dir).
///
/// Existing process variables are never overridden (dotenvy semantics), so `.env` sits below
/// the process environment in the precedence chain: what the file adds is visible to a
/// consumer that reads the environment *after* this call — the CLI's authoritative parse —
/// while a variable the operator exported keeps its own value.
///
/// A malformed file is an error, never a silently ignored one: FR-016 is fail-fast, and a
/// typo in a `.env` a run depends on is exactly the failure a silent skip hides.
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
    parse_switch(raw).map_err(|refusal| Error::invalid_value(READ_ONLY_VAR, refusal))
}

/// The environment variable that selects sandbox mode — decision 1's third
/// channel, beside the `--sandbox` flag and the `[server] sandbox` key.
pub const SANDBOX_VAR: &str = "TINIO_SANDBOX";

/// Parse a `TINIO_SANDBOX` value: `1`/`true` and `0`/`false` only. Anything
/// else is a startup error — the same strictness as [`parse_read_only`] and
/// for the same reason: the value decides whether the process confines
/// itself, and a typo that fell back to the default would leave an operator
/// who wrote `TINIO_SANDBOX=yes` serving unconfined with nothing logged
/// (design decision 5, §11).
pub fn parse_sandbox(raw: &str) -> Result<bool, Error> {
    parse_switch(raw).map_err(|refusal| Error::invalid_value(SANDBOX_VAR, refusal))
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

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

    #[test]
    fn sandbox_flag_parses_strictly() {
        // The same grammar and the same strictness as `TINIO_READ_ONLY`, and
        // for the stronger version of the same reason: this variable decides
        // whether the run confines itself, so `TINIO_SANDBOX=yes` answering
        // "off" would be an operator who believes they are confined and is
        // not (design decision 5, §11).
        for raw in ["1", "true", " 1 "] {
            assert!(parse_sandbox(raw).unwrap(), "{raw}");
        }
        for raw in ["0", "false"] {
            assert!(!parse_sandbox(raw).unwrap(), "{raw}");
        }
        // An empty value is an error, never "unset" or "false".
        for raw in ["", "yes"] {
            let err = parse_sandbox(raw).unwrap_err();
            assert!(err.to_string().contains(SANDBOX_VAR), "{raw}: {err}");
        }
    }
}
