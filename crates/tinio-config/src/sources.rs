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
//!   flags > env > `.env` falls out of clap's own precedence — there is no
//!   manual env overlay here;
//! - the config file is the lowest-precedence source: parsed by
//!   [`crate::schema::Config::parse`] as the base, overlaid by the CLI.
//!
//! The `MINIO_*` credential fallback (T084) and `TINIO_ANONYMOUS` (T087)
//! extend this module in US3.

use std::path::Path;

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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn load_env_file_reads_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        // Unique name: a lingering value from a previous failed run would
        // only fail this test's assertion, never affect other tests.
        let var = "TINIO_TEST_ENV_FILE_LOADED";
        fs::write(dir.path().join(".env"), format!("{var}=yes\n")).unwrap();
        load_env_file(dir.path()).unwrap();
        assert_eq!(std::env::var(var).unwrap(), "yes");
    }

    #[test]
    fn load_env_file_absent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_env_file(dir.path()).is_ok());
    }
}
