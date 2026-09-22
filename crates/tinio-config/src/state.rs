//! State-directory resolution (FR-023).
//!
//! Normal mode keeps the tool's private state inside the served root
//! (`<root>/.tinio/`, resolved by the backend). Read-only mode must not
//! write the root at all — it may be a genuinely read-only filesystem — so
//! every piece of tool state moves under the user's home:
//! `<home>/.tinio/roots/<hash>/` (contracts/config.md; data-model.md
//! "Reserved Directory").

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::_core::object::RESERVED_SEGMENT;
use crate::{Error, error};

/// Length of the derived directory name, in hexadecimal characters.
const HASH_LEN: usize = 16;
/// The tool's home directory name, spelled by `tinio-core`'s own constant so the home layout
/// and the storage layout cannot diverge.
pub(crate) const BASE_DIR_NAME: &str = RESERVED_SEGMENT;
/// The per-root state directory parent.
const ROOTS_DIR_NAME: &str = "roots";

/// `<home>/.tinio/roots/<sha256(canonical root)16>/` (FR-023).
///
/// `home` is an explicit parameter, never `dirs::home_dir()` inline: tests
/// must resolve a scratch home instead of the real one.
///
/// The root is canonicalized first — a root's identity is its canonical
/// path — so a root that does not exist fails here rather than silently
/// minting a new identity. The two parents are created mode 0700 on unix
/// (otherwise they inherit the umask, letting other users enumerate roots);
/// the leaf is left to the backend, which creates it 0700 already
/// (`tinio-fs`'s state-database open).
pub fn home_state_dir(root: &Path, home: &Path) -> Result<PathBuf, Error> {
    let canonical = root.canonicalize().map_err(|e| error::root(root, e))?;
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let name = &hex::encode(digest)[..HASH_LEN];
    Ok(ensure_roots(home)?.join(name))
}

/// [`home_state_dir`] over the real home directory.
pub fn default_home_state_dir(root: &Path) -> Result<PathBuf, Error> {
    let home = dirs::home_dir().ok_or_else(|| Error::Missing("home directory".into()))?;
    home_state_dir(root, &home)
}

/// The environment variable that overrides the derived state dir (FR-023).
///
/// **The name lives here and the reading lives in the CLI**, which is what keeps one owner per
/// variable: this constant is what [`parse_state_dir`] refuses an empty value by, and that
/// parser is the `value_parser` of the CLI's environment-only `--state-dir` argument. A caller
/// that wants the value reads it from the parse, never from the environment.
///
/// What the variable means is the contract's (`contracts/config.md`'s read-only section): an
/// explicit state dir used verbatim instead of the `~/.tinio/roots/<hash>/` derivation — the
/// escape hatch for the environments read-only mode is actually for (service accounts,
/// containers, immutable images) where the home may be absent or unwritable, and where
/// [`default_home_state_dir`] would otherwise fail startup outright. **It is consulted in both
/// modes, and what it moves differs between them.** A read-only run's served state dir *is*
/// this value when it is set, and the pair such a client probes is `[this value, <root>/.tinio]`,
/// so the home-derived candidate is never derived there at all. A read-write run's served dir
/// stays `<root>/.tinio` — the CLI's `resolve::resolve_state_dir` never consults it on that path
/// — but the variable answers in place of the second derivation a client makes there, the
/// home-derived one, which is why a machine with no usable home needs no second variable to be
/// reported on. An exported value is therefore never silently ignored: in read-write mode it
/// moves what `status`, `stop` and `doctor` look at second, and nothing else.
pub const STATE_DIR_VAR: &str = "TINIO_STATE_DIR";

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
/// use tinio_config::state::parse_state_dir;
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

/// Refuse a symlinked `<home>/.tinio` or `<home>/.tinio/roots`.
///
/// Normal mode already refuses a symlinked `<root>/.tinio` (`prove_contained`),
/// and the same posture applies here: following the link would put the tool's
/// state — and with it the promise that the served root is untouched —
/// wherever the link points. `TINIO_STATE_DIR` is the escape hatch for a
/// deliberate layout.
///
/// The refusal is an [`Error::Root`], not an [`Error::InvalidValue`]: the
/// subject is a path in the state-directory layout, never a config key.
fn refuse_symlink(dir: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() => Err(error::root(
            dir,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "must not be a symlink (normal mode refuses a symlinked `<root>/.tinio` \
                 for the same reason; set TINIO_STATE_DIR for an explicit location)",
            ),
        )),
        _ => Ok(()),
    }
}

/// Create `<home>/.tinio` and `<home>/.tinio/roots` (mode 0700 on unix)
/// and return the latter. A path that already exists as something other
/// than a directory — a regular file left where the layout needs a
/// directory — is an error, not a tolerated `AlreadyExists`.
fn ensure_roots(home: &Path) -> Result<PathBuf, Error> {
    let base = home.join(BASE_DIR_NAME);
    let roots = base.join(ROOTS_DIR_NAME);
    for dir in [&base, &roots] {
        refuse_symlink(dir)?;
        // A concurrent instance may create the same directory between the
        // probe and the create; AlreadyExists is the only tolerated error.
        if let Err(e) = create_dir_0700(dir)
            && e.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(error::root(dir, e));
        }
        // A pre-existing regular file lands here too: `create_dir` answers
        // `AlreadyExists` for it and `set_permissions` succeeds on a file,
        // so without this check the resolver would answer `Ok` for a path
        // that can never work, and the failure would only surface later as
        // `ENOTDIR` inside the backend. Checked before the mode below, so a
        // file that is not ours is never chmodded.
        if !dir.is_dir() {
            return Err(error::root(
                dir,
                io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "exists and is not a directory",
                ),
            ));
        }
        // The mode is enforced unconditionally, not only at creation: a
        // `<home>/.tinio` an earlier version created 0755 would otherwise
        // stay world-enumerable forever, and the whole read-only story
        // rests on other users not seeing this directory. Same shape as
        // `tinio-fs`'s state-db open, which always sets the leaf's mode.
        enforce_0700(dir)?;
    }
    Ok(roots)
}

#[cfg(unix)]
fn enforce_0700(dir: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|e| error::root(dir, e))
}

#[cfg(not(unix))]
fn enforce_0700(_dir: &Path) -> Result<(), Error> {
    Ok(())
}

#[cfg(unix)]
fn create_dir_0700(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new().mode(0o700).create(dir)
}

#[cfg(not(unix))]
fn create_dir_0700(dir: &Path) -> io::Result<()> {
    fs::create_dir(dir)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn layout_is_home_tinio_roots_hash() {
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let state = home_state_dir(root.path(), home.path()).unwrap();
        assert_eq!(
            state.parent().unwrap(),
            home.path().join(".tinio").join("roots")
        );
        let leaf = state.file_name().unwrap().to_str().unwrap();
        assert_eq!(leaf.len(), 16, "16 hex characters: {leaf}");
        assert!(leaf.chars().all(|c| c.is_ascii_hexdigit()), "{leaf}");
        assert!(state.parent().unwrap().is_dir(), "parents are created");
    }

    #[test]
    fn hash_is_stable_and_root_specific() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        assert_eq!(
            home_state_dir(a.path(), home.path()).unwrap(),
            home_state_dir(a.path(), home.path()).unwrap()
        );
        assert_ne!(
            home_state_dir(a.path(), home.path()).unwrap(),
            home_state_dir(b.path(), home.path()).unwrap()
        );
    }

    #[test]
    fn missing_root_is_an_error() {
        let home = tempfile::tempdir().unwrap();
        let missing = home.path().join("nope");
        let err = home_state_dir(&missing, home.path()).unwrap_err();
        assert!(matches!(err, Error::Root { .. }), "{err:?}");
    }

    #[test]
    fn a_file_where_the_layout_needs_a_directory_is_refused() {
        // A regular file at `<home>/.tinio/roots`: `create_dir` answers
        // `AlreadyExists` (tolerated) and `set_permissions` succeeds on a
        // file, so without the `is_dir` check the resolver would answer
        // `Ok` for a path the backend can only fail on with `ENOTDIR`.
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let base = home.path().join(".tinio");
        fs::create_dir(&base).unwrap();
        fs::write(base.join("roots"), "not a directory").unwrap();
        let err = home_state_dir(root.path(), home.path()).unwrap_err();
        assert!(matches!(err, Error::Root { .. }), "{err:?}");
        assert!(err.to_string().contains("roots"), "{err}");
        assert!(err.to_string().contains("not a directory"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn parents_are_mode_0700() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let state = home_state_dir(root.path(), home.path()).unwrap();
        for dir in [
            home.path().join(".tinio"),
            state.parent().unwrap().to_path_buf(),
        ] {
            let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_pre_existing_loose_parent_is_tightened() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let base = home.path().join(".tinio");
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        home_state_dir(root.path(), home.path()).unwrap();
        let mode = fs::metadata(&base).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "an inherited 0755 must be tightened");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_parent_is_refused() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        symlink(elsewhere.path(), home.path().join(".tinio")).unwrap();
        let err = home_state_dir(root.path(), home.path()).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");
    }

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
