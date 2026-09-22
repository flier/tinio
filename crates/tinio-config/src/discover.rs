//! Finding the configuration file, and creating it on a first start (FR-007, `config.md:9,120`).
//!
//! The search order is design decision 5. Its last two steps are what make read-only mode
//! ordinary: the state directory *is* `<root>/.tinio` in read-write mode, and in read-only
//! mode it is the relocated home directory, which is exactly where an earlier read-only run
//! auto-created the file.

#[cfg(not(windows))]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(windows)]
use crate::_winsec::{create_restricted, restrict_to_current_user as restrict_file};
use crate::{Config, Error, credentials, error, state::BASE_DIR_NAME};

/// The primary file's name inside a state directory.
///
/// # Examples
///
/// ```rust
/// use tinio_config::discover::CONFIG_FILE;
///
/// assert_eq!(CONFIG_FILE, ".tinio.toml");
/// ```
pub const CONFIG_FILE: &str = ".tinio.toml";

/// The alias accepted when the primary name is absent.
///
/// # Examples
///
/// ```rust
/// use tinio_config::discover::CONFIG_ALIAS;
///
/// assert_eq!(CONFIG_ALIAS, "config.toml");
/// ```
pub const CONFIG_ALIAS: &str = "config.toml";

/// Whether the file was already there.
///
/// # Examples
///
/// ```rust
/// use tinio_config::discover::Outcome;
///
/// assert_ne!(Outcome::Created, Outcome::Loaded);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// An existing file was parsed.
    Loaded,
    /// A new file was written with generated credentials.
    Created,
}

/// The configuration file for `root`, searched in decision 5's order.
///
/// An explicit path is returned verbatim and must already be a file: the caller refuses a
/// `--config` that names a missing one with exit 2 (Task 2's `resolve::config_path`), which is
/// what keeps the create branch below to discovery's single input.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_config::discover::resolve_path;
///
/// let path = resolve_path(Path::new("/srv/data"), Path::new("/srv/data/.tinio"), None)?;
/// # Ok::<(), tinio_config::Error>(())
/// ```
pub fn resolve_path(
    root: &Path,
    state_dir: &Path,
    explicit: Option<&Path>,
) -> Result<PathBuf, Error> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    // The two directories are the same path in read-write mode (`<root>/.tinio` *is* the state
    // dir), so probing both would stat every candidate twice for the same answer.
    let reserved = root.join(BASE_DIR_NAME);
    let directories = if reserved == state_dir {
        [Some(reserved), None]
    } else {
        [Some(reserved), Some(state_dir.to_path_buf())]
    };
    for directory in directories.into_iter().flatten() {
        for name in [CONFIG_FILE, CONFIG_ALIAS] {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    Ok(state_dir.join(CONFIG_FILE))
}

/// Reads `path`, or writes it with generated credentials when it is absent.
///
/// Auto-create is discovery's, and its one reachable input is `<state_dir>.tinio.toml`: an
/// operator-named path never arrives absent, and read-only mode refuses a path under `root`
/// outright, because FR-023's contract is that nothing under the served root is ever written.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_config::discover::{Outcome, load_or_create, resolve_path};
///
/// let root = Path::new("/srv/data");
/// let path = resolve_path(root, Path::new("/srv/data/.tinio"), None)?;
/// let (_config, outcome) = load_or_create(root, &path, false)?;
/// assert!(matches!(outcome, Outcome::Loaded | Outcome::Created));
/// # Ok::<(), tinio_config::Error>(())
/// ```
pub fn load_or_create(
    root: &Path,
    path: &Path,
    read_only: bool,
) -> Result<(Config, Outcome), Error> {
    if path.is_file() {
        return Ok((Config::load(path)?, Outcome::Loaded));
    }
    if read_only && path.starts_with(root) {
        return Err(Error::InvalidValue {
            key: "server.read_only".into(),
            reason: format!(
                "read-only mode never writes under the served root, and {} is inside {}",
                path.display(),
                root.display()
            ),
        });
    }
    let config = Config::generated(read_only, credentials::generate()?)?;
    let text = config.to_toml()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| error::io(parent, err))?;
    }
    let mut file = create_private(path)?;
    file.write_all(text.as_bytes())
        .map_err(|err| error::io(path, err))?;
    #[cfg(windows)]
    restrict_or_remove(path, restrict_to_current_user)?;
    Ok((config, Outcome::Created))
}

/// Creates `path` — which must not exist yet — with the created config's protection applied **at
/// creation**, and hands back the write handle.
///
/// Two platforms, two mechanisms, one promise. On unix the promise is the open call's own
/// argument (`OpenOptions::mode`): the file never exists with a mode wider than `0o600`. On
/// Windows it is `tinio_winsec::create_restricted`: the DACL is attached when the file is
/// created, because Windows decides access when a handle is **opened** — a file that existed
/// under its directory's inherited grants for even a moment could be opened by a reader that
/// keeps the handle's access, and this file holds the credentials. `restrict_or_remove` still
/// runs after the write as the re-assertion.
///
/// Both arms refuse an existing path (`create_new` / `CREATE_NEW`), so nothing here can adopt a
/// file that appeared between the `is_file` test above and this call.
fn create_private(path: &Path) -> Result<fs::File, Error> {
    #[cfg(windows)]
    {
        create_restricted(path).map_err(|err| error::io(path, err))
    }
    #[cfg(not(windows))]
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        options.open(path).map_err(|err| error::io(path, err))
    }
}

/// Runs the Windows re-assertion after the create and **removes the file when it fails**.
///
/// The DACL already travelled in `tinio_winsec::create_restricted`, and this call is what refuses
/// a descriptor that did not take — the same second line the management plane's state write runs
/// before its first byte.
///
/// **A file whose restriction failed must not survive the run.** It holds the complete `[auth]`
/// pair, and under the directory's inherited grants every account the parent grants can read it;
/// leaving it behind would also make the exposure *persist*, because the next run finds the file,
/// parses it and answers [`Outcome::Loaded`] — the pair would be served out of a file nothing ever
/// narrows again. The removal is best-effort: the caller is answered the failure that explains why
/// the config was not published, never a second one from the cleanup.
///
/// `restrict` is a parameter so a unit leg can drive the failure branch — `SetNamedSecurityInfoW`
/// cannot be made to fail on a file this process owns (an owner may always write its own DACL), so
/// the branch has no other way to be reached. The production call site passes
/// [`restrict_to_current_user`].
#[cfg(windows)]
fn restrict_to_current_user(path: &Path) -> Result<(), Error> {
    restrict_file(path).map_err(|err| error::io(path, err))
}

#[cfg(windows)]
fn restrict_or_remove(
    path: &Path,
    restrict: impl FnOnce(&Path) -> Result<(), Error>,
) -> Result<(), Error> {
    match restrict(path) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = fs::remove_file(path);
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use secrecy::ExposeSecret;
    use tempfile::TempDir;

    #[cfg(windows)]
    use crate::_winsec::acl_is_current_user_only;

    use super::*;

    /// Seeds `<dir>/<rel>`, parents included, and returns the path.
    fn seeded(dir: &Path, rel: &str) -> PathBuf {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent");
        }
        fs::write(&path, "version = 1\n").expect("seed");
        path
    }

    #[test]
    fn the_root_config_wins_over_the_state_dirs() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        // Decision 5's order, first hit wins: `<root>/.tinio/.tinio.toml` leads.
        let primary = seeded(root.path(), ".tinio/.tinio.toml");
        let _alias = seeded(root.path(), ".tinio/config.toml");
        let _state_copy = seeded(state.path(), ".tinio.toml");

        assert_eq!(
            resolve_path(root.path(), state.path(), None).expect("resolve"),
            primary
        );
    }

    #[test]
    fn the_alias_is_read_and_the_primary_name_beats_it() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        // Only the alias is there: it answers, in the root and in the state dir alike.
        let root_alias = seeded(root.path(), ".tinio/config.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), None).expect("resolve"),
            root_alias
        );

        fs::remove_file(&root_alias).expect("unseed");
        let state_alias = seeded(state.path(), "config.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), None).expect("resolve"),
            state_alias
        );

        // And within one directory the primary name wins over the alias.
        let state_primary = seeded(state.path(), ".tinio.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), None).expect("resolve"),
            state_primary
        );
    }

    #[test]
    fn the_state_dir_is_the_fallback() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let state_copy = seeded(state.path(), ".tinio.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), None).expect("resolve"),
            state_copy
        );
    }

    #[test]
    fn an_explicit_path_is_used_verbatim() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let explicit = seeded(root.path(), "elsewhere.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), Some(&explicit)).expect("resolve"),
            explicit
        );
    }

    #[cfg(windows)]
    #[test]
    fn a_restriction_that_fails_takes_the_file_with_it() {
        // The other half of the create path: a config whose post-create restriction failed must
        // not survive the run. It holds the complete `[auth]` pair under whatever the directory
        // grants, and a later run would find it, parse it and answer `Outcome::Loaded` — so the
        // exposure would persist rather than end with the failing run.
        // `SetNamedSecurityInfoW` cannot be made to fail on a file this process owns (an owner may
        // always write its own DACL), which is why `restrict_or_remove` takes the restriction as a
        // parameter and this leg drives that seam.
        let dir = TempDir::new().expect("dir");
        let path = dir.path().join(CONFIG_FILE);
        fs::write(&path, "version = 1\n").expect("seed");
        let err = restrict_or_remove(&path, |_| {
            Err(Error::InvalidValue {
                key: "test".into(),
                reason: "the restriction did not take".into(),
            })
        })
        .expect_err("the failure is reported rather than swallowed");
        assert!(format!("{err}").contains("did not take"), "{err}");
        assert!(
            !path.exists(),
            "a config whose restriction failed must not be left on disk"
        );

        // And the production pair, on a file that is there: the restriction takes and the file
        // stays, which is what every ordinary first start does.
        let kept = dir.path().join("kept.toml");
        fs::write(&kept, "version = 1\n").expect("seed");
        restrict_or_remove(&kept, restrict_to_current_user).expect("the restriction took");
        assert!(kept.exists(), "a restricted config is kept");
    }

    #[test]
    fn a_first_start_creates_a_config_with_credentials_and_the_local_api_channel() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        let (config, outcome) = load_or_create(root.path(), &path, false).expect("create");

        assert!(matches!(outcome, Outcome::Created));
        assert!(path.exists());
        let auth = config.auth.as_ref().expect("generated credentials");
        assert!(auth.access_key.len() >= 16);
        assert!(auth.secret_key.expose_secret().len() >= 32);
        assert_eq!(config.server.port, 9000);
        assert!(!config.server.read_only);

        // The local management channel is written too, and it is written *explicitly*:
        // Task 2's `api_options` would enable the derived channel for a config with no
        // `[api]` section at all, so the section is not what makes `status` possible —
        // it is what makes the channel visible and editable in the file an operator
        // reads, with the derived path spelled out rather than implied.
        let text = fs::read_to_string(&path).expect("read back");
        let local = if cfg!(windows) {
            "[api.pipe]"
        } else {
            "[api.unix]"
        };
        assert!(
            text.contains(local),
            "the local channel is part of the created config:\n{text}"
        );

        // The second call reads what the first wrote rather than generating again.
        let (again, outcome) = load_or_create(root.path(), &path, false).expect("load");
        assert!(matches!(outcome, Outcome::Loaded));
        assert_eq!(
            again.auth.as_ref().expect("credentials").access_key,
            auth.access_key
        );
    }

    #[test]
    fn a_read_only_root_creates_its_config_in_the_state_dir() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        let (config, outcome) = load_or_create(root.path(), &path, true).expect("create");
        assert!(matches!(outcome, Outcome::Created));
        assert!(
            path.starts_with(state.path()),
            "read-only never writes the served root"
        );
        assert!(
            config.server.read_only,
            "the mode is recorded, so the next start keeps it"
        );
    }

    #[test]
    fn a_read_only_run_refuses_to_create_under_the_served_root() {
        // `TINIO_STATE_DIR` inside the root is the reachable case: read-only mode relocated
        // everything, so a state dir under the root is a contradiction, and the guard makes
        // it a refusal rather than a write into the root.
        let root = TempDir::new().expect("root");
        let inside = root.path().join(".tinio");
        let err = load_or_create(root.path(), &inside.join(CONFIG_FILE), true)
            .expect_err("read-only never writes the root");
        assert!(format!("{err}").contains("read-only"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn the_created_config_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        load_or_create(root.path(), &path, false).expect("create");
        let mode = fs::metadata(&path).expect("metadata").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "it holds the credentials");
    }

    #[cfg(windows)]
    #[test]
    fn the_created_config_is_private_on_windows() {
        // A second account's absence cannot be tested from a single-account runner, so the
        // read-back answers whether every ACE is the current user's full control and nothing
        // is inherited.
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        load_or_create(root.path(), &path, false).expect("create");
        assert!(
            acl_is_current_user_only(&path).expect("DACL"),
            "only the current user, full control, nothing inherited"
        );

        // And the create-time form alone, with no path-form call after it: the descriptor
        // travels in the create, so this is the leg that would fail if the create silently
        // kept the directory's inherited grants — which the run above cannot show, because its
        // re-assertion would have narrowed the DACL either way.
        let direct = state.path().join("direct.toml");
        let _file = create_restricted(&direct).expect("created with the descriptor");
        assert!(
            acl_is_current_user_only(&direct).expect("DACL"),
            "the file exists with the current user's ACE and nothing inherited"
        );
    }
}
