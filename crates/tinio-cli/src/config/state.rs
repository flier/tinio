//! `TINIO_STATE_DIR` parsing — the `--state-dir` argument's `value_parser`.

use std::path::PathBuf;

use crate::_util::{env::STATE_DIR_VAR, state::Error};

/// Parse a `TINIO_STATE_DIR` value — the CLI's `--state-dir` argument's parser, and the one
/// place the value's shape is decided.
///
/// An empty value is an error, never "the current directory"; the trimmed value is the path.
/// The path is taken verbatim: its parents are **not** created mode 0700 and no symlink check
/// applies, because the caller chose the location explicitly — the backend still creates the
/// leaf itself and sets it 0700.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::state::parse_state_dir;
///
/// assert_eq!(
///     parse_state_dir(" /srv/tinio-state ")
///         .expect("a path")
///         .to_str(),
///     Some("/srv/tinio-state")
/// );
/// ```
pub fn parse_state_dir(raw: &str) -> Result<PathBuf, Error> {
    let path = raw.trim();
    if path.is_empty() {
        return Err(Error::invalid_value(STATE_DIR_VAR, "must not be empty"));
    }
    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_dir_override_parses_strictly() {
        assert_eq!(
            parse_state_dir(" /srv/tinio-state ").unwrap(),
            PathBuf::from("/srv/tinio-state")
        );
        let err = parse_state_dir("   ").unwrap_err();
        assert!(err.to_string().contains(STATE_DIR_VAR), "{err}");
    }
}
