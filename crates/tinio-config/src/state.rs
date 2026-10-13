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

use crate::{_core::object::RESERVED_SEGMENT, Error, error};

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
    let name = derived_name(root)?;
    Ok(ensure_roots(home)?.join(name))
}

/// [`home_state_dir`] over the real home directory — the creating form's public entry, part of
/// the resolver interface the read-only design settles (`2026-09-18-read-only-mode-design.md`
/// decision 16 and its interface list).
///
/// **No CLI path calls it**, and that is deliberate rather than dead: a client command resolves
/// the *non-creating* [`home_state_dir_path`] (a probe must not mint the tree it is asking
/// about), and a command that goes on to write the leaf hardens it through
/// [`prepare_home_derived`] before its own `create_dir_all`. This is the form for a caller that
/// wants the deriving **and** the creation in one step.
pub fn default_home_state_dir(root: &Path) -> Result<PathBuf, Error> {
    let home = dirs::home_dir().ok_or_else(|| Error::Missing("home directory".into()))?;
    home_state_dir(root, &home)
}

/// [`home_state_dir`]'s path **without its filesystem side effects**: the same canonicalization
/// and the same `<home>/.tinio/roots/<sha256(canonical root)16>/` computation, with nothing
/// created, validated or chmodded.
///
/// A **client** command derives this candidate only to look for a server that might be there —
/// `status`, `stop` and `doctor` must not create the home tree they are merely asking about — so
/// the deriving half is split from `ensure_roots`. A **serving** command still resolves through
/// [`home_state_dir`], whose creation is a write it is about to make anyway.
pub fn home_state_dir_path(root: &Path, home: &Path) -> Result<PathBuf, Error> {
    let name = derived_name(root)?;
    Ok(roots_dir(home).1.join(name))
}

/// The two levels of the home layout, `(<home>/.tinio, <home>/.tinio/roots)`: the one composition
/// [`home_state_dir_path`] and [`ensure_roots`] share, so the creating and the non-creating form
/// cannot end up pointing at different trees.
fn roots_dir(home: &Path) -> (PathBuf, PathBuf) {
    let base = home.join(BASE_DIR_NAME);
    let roots = base.join(ROOTS_DIR_NAME);
    (base, roots)
}

/// [`home_state_dir_path`] over the real home directory.
pub fn default_home_state_dir_path(root: &Path) -> Result<PathBuf, Error> {
    let home = dirs::home_dir().ok_or_else(|| Error::Missing("home directory".into()))?;
    home_state_dir_path(root, &home)
}

/// If `state_dir` is this root's home-derived leaf, create and harden `<home>/.tinio` and
/// `roots` (0700 on unix, refuse symlinks). No-op for any other path — reserved `.tinio`, an
/// explicit `TINIO_STATE_DIR`, etc.
///
/// Clients resolve through [`home_state_dir_path`] so a probe creates nothing. Serving and
/// `--fix` still `create_dir_all` the leaf; without this call the parents inherit the umask and
/// follow a planted symlink. Call it once, immediately before that create.
pub fn prepare_home_derived(root: &Path, state_dir: &Path) -> Result<(), Error> {
    let Some(home) = dirs::home_dir() else {
        return Ok(());
    };
    prepare_home_derived_in(root, state_dir, &home)
}

/// [`prepare_home_derived`] with an explicit home — the tests below pin a scratch home so they
/// never touch the developer's; production goes through [`prepare_home_derived`], which is why
/// this is private rather than a second public entry point.
///
/// **A root that cannot be canonicalized is not this function's subject.** The derivation *is*
/// the canonicalization, so a root the serving half has not created yet answers with nothing to
/// compare and nothing to harden — which is exactly a read-only pg run's first start: its state
/// directory is `<synthetic root>/.tinio`, a leaf nothing has created, and before this arm the
/// call failed the run on a `canonicalize` of a path that run was about to create itself.
fn prepare_home_derived_in(root: &Path, state_dir: &Path, home: &Path) -> Result<(), Error> {
    let Ok(derived) = home_state_dir_path(root, home) else {
        return Ok(());
    };
    if state_dir != derived {
        return Ok(());
    }
    let _ = ensure_roots(home)?;
    Ok(())
}

/// Create and harden a **pg run's synthetic root** — `<root>` and its reserved `.tinio` state
/// directory — mode 0700 on unix, refusing a symlinked component.
///
/// **A pg run's state directory is the one leaf no backend hardens.** A filesystem run's
/// `<root>/.tinio` is created mode 0700 by `tinio-fs`'s state-database open, and a read-only
/// run's leaf is hardened by [`prepare_home_derived`]; a pg run's store is the database, so
/// nothing but this call owns that directory — and a bare `create_dir_all` would leave the
/// tool's own state (and the access log beside it) world-enumerable on a 022 umask.
///
/// **It is called immediately before the serving half's `create_dir_all`s**, the same position
/// [`prepare_home_derived`] documents, so the directories exist and are already 0700 by the time
/// those calls are no-ops. A missing parent is **created** 0700 — the XDG chain
/// (`<home>/.local/state/tinio/pg/`) is the tool's own — while an existing one is left with the
/// mode it has, so a `TINIO_STATE_DIR` the operator named keeps the shape they gave it apart
/// from the two levels below, which are the tool's state and are enforced. The symlink refusal
/// is the read-only leaf's rule for the same reason: following a planted link would put the
/// tool's state wherever the link points.
pub fn prepare_synthetic_root(root: &Path) -> Result<(), Error> {
    let state_dir = root.join(RESERVED_SEGMENT);
    create_dir_all_0700(&state_dir)?;
    for dir in [root, state_dir.as_path()] {
        refuse_pg_symlink(dir)?;
        if !dir.is_dir() {
            return Err(error::root(
                dir,
                io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "exists and is not a directory",
                ),
            ));
        }
        enforce_0700(dir)?;
    }
    Ok(())
}

/// [`prepare_synthetic_root`]'s symlink refusal, worded for the synthetic root rather than for
/// the home-derived layout: the escape a locked-out operator has here is the dsn's own root
/// (drop the symlink) or a `TINIO_STATE_DIR` pointing at a real directory.
fn refuse_pg_symlink(dir: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() => Err(error::root(
            dir,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "must not be a symlink (a pg run's synthetic root is the tool's own state; name \
                 a real directory with TINIO_STATE_DIR, or a different dsn)",
            ),
        )),
        _ => Ok(()),
    }
}

/// Create `dir` and every missing ancestor **mode 0700 on unix**, the hardening
/// [`ensure_roots`] gives the home layout's two levels, generalized to a path the tool creates
/// whole (a pg run's `<home>/.local/state/tinio/pg/<identity>/`).
///
/// A component that already exists is left alone: unlike the two levels `ensure_roots` owns, an
/// ancestor here may be the user's own (`~/.local`), which this tool must not chmod.
fn create_dir_all_0700(dir: &Path) -> Result<(), Error> {
    let missing: Vec<&Path> = dir
        .ancestors()
        .filter(|dir| !dir.as_os_str().is_empty() && !dir.exists())
        .collect();
    for next in missing.into_iter().rev() {
        // A concurrent instance may create the same directory between the probe and the create;
        // AlreadyExists is the only tolerated error (the shape `ensure_roots` documents).
        if let Err(e) = create_dir_0700(next)
            && e.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(error::root(next, e));
        }
    }
    Ok(())
}

/// The per-root leaf name (`<sha256(canonical root)16>`), the one computation [`home_state_dir`]
/// and [`home_state_dir_path`] share so the creating and the non-creating form cannot disagree.
fn derived_name(root: &Path) -> Result<String, Error> {
    let canonical = root.canonicalize().map_err(|e| error::root(root, e))?;
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    Ok(hex::encode(digest)[..HASH_LEN].to_string())
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
/// containers, immutable images) where the home may be absent or unwritable, and where the
/// home-derived state dir cannot be created at all — [`prepare_home_derived`], which is what a
/// serving or `--fix` run calls before its own `create_dir_all`, or [`default_home_state_dir`],
/// which derives and creates in one step. **It is consulted in both
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
    let (base, roots) = roots_dir(home);
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
    fn the_non_creating_form_computes_the_same_path_and_creates_nothing() {
        // A client derives the alternate candidate only to look: `status`, `stop` and `doctor`
        // must not create `<home>/.tinio/roots/` merely by being run.
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let quiet = home_state_dir_path(root.path(), home.path()).unwrap();
        assert!(
            !home.path().join(".tinio").exists(),
            "the deriving form must not create the home tree"
        );
        let loud = home_state_dir(root.path(), home.path()).unwrap();
        assert_eq!(quiet, loud, "the two forms compute one path");
        assert!(home.path().join(".tinio/roots").is_dir());
    }

    #[test]
    fn prepare_home_derived_hardens_only_the_home_leaf() {
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let reserved = root.path().join(".tinio");
        prepare_home_derived_in(root.path(), &reserved, home.path()).unwrap();
        assert!(
            !home.path().join(".tinio").exists(),
            "a non-home leaf must not mint the home tree"
        );
        let derived = home_state_dir_path(root.path(), home.path()).unwrap();
        prepare_home_derived_in(root.path(), &derived, home.path()).unwrap();
        assert!(
            home.path().join(".tinio/roots").is_dir(),
            "the home leaf's parents must exist after prepare"
        );
        assert_eq!(
            derived,
            home.path()
                .join(".tinio")
                .join("roots")
                .join(derived.file_name().unwrap())
        );
    }

    #[cfg(unix)]
    #[test]
    fn prepare_home_derived_refuses_a_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        symlink(elsewhere.path(), home.path().join(".tinio")).unwrap();
        let derived = home_state_dir_path(root.path(), home.path()).unwrap();
        let err = prepare_home_derived_in(root.path(), &derived, home.path()).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    /// **A root the serving half has not created yet is no home-derived leaf, and must not fail
    /// the run that is about to create it** (ruling R8's read-only half). The derivation is the
    /// canonicalization, so the first start of a read-only pg run — its state directory being
    /// `<synthetic root>/.tinio`, a leaf nothing has made — reached a `canonicalize` of a
    /// missing path and refused before anything was created. The home tree stays untouched
    /// either way: there is nothing to harden.
    #[test]
    fn a_root_that_does_not_exist_yet_is_not_hardened_and_not_refused() {
        let home = tempfile::tempdir().unwrap();
        let missing = home.path().join("not-created-yet");
        let state_dir = missing.join(".tinio");
        prepare_home_derived_in(&missing, &state_dir, home.path())
            .expect("a first start resolves a root it has not created");
        assert!(
            !home.path().join(".tinio").exists(),
            "nothing may be minted in the home for a root that is not a home-derived leaf"
        );
        assert!(!missing.exists(), "and nothing is created here either");
    }

    /// **A pg run's synthetic root is created hardened** (the finding the read-only posture left
    /// open): its state directory is a leaf no backend opens, so without
    /// [`prepare_synthetic_root`] it — and the access log inside it — would inherit the process
    /// umask (0755 on a 022 umask) and any local user could enumerate the tool's state.
    #[cfg(unix)]
    #[test]
    fn a_synthetic_root_is_created_and_hardened() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::tempdir().unwrap();
        let root = home
            .path()
            .join(".local")
            .join("state")
            .join("tinio")
            .join("pg")
            .join("85b95498583db52c");
        prepare_synthetic_root(&root).unwrap();

        for dir in [root.clone(), root.join(".tinio")] {
            let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
        // The XDG parents this call created are private too, and an ancestor that already
        // existed keeps its own mode (`~/.local` is the user's, not this tool's).
        let tool_parents = home
            .path()
            .join(".local")
            .join("state")
            .join("tinio")
            .join("pg");
        let mode = fs::metadata(&tool_parents).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{}", tool_parents.display());
        assert_eq!(
            fs::metadata(home.path().join(".local"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "a missing ancestor is created 0700 rather than with the umask"
        );
    }

    /// The other half of the same finding: the two levels the tool owns are enforced whatever
    /// mode they were found in (the read-only leaf's rule), an ancestor that already exists is
    /// left alone, and a symlinked component is refused rather than followed.
    #[cfg(unix)]
    #[test]
    fn a_synthetic_root_enforces_its_own_levels_and_refuses_a_symlink() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let dir = tempfile::tempdir().unwrap();
        // An ancestor the user made keeps its mode: it is not this tool's to tighten.
        let users_own = dir.path().join("mine");
        fs::create_dir(&users_own).unwrap();
        fs::set_permissions(&users_own, fs::Permissions::from_mode(0o755)).unwrap();
        let root = users_own.join("pg-root");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
        prepare_synthetic_root(&root).unwrap();
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700,
            "the synthetic root is the tool's state and is enforced 0700"
        );
        assert_eq!(
            fs::metadata(&users_own).unwrap().permissions().mode() & 0o777,
            0o755,
            "an ancestor that already exists is left with the mode it was given"
        );

        let elsewhere = tempfile::tempdir().unwrap();
        let linked = dir.path().join("linked");
        symlink(elsewhere.path(), &linked).unwrap();
        let err = prepare_synthetic_root(&linked).unwrap_err();
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
