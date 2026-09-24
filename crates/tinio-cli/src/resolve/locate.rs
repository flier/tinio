//! Root, mode and state-dir location (FR-016 steps a-d).

use std::{
    env,
    path::{Path, PathBuf},
};

use clap::parser::ValueSource;

use crate::{
    _config::{Config, discover, sources, state},
    _core::object::RESERVED_SEGMENT,
    Error,
    cli::{Discovery, Provenance},
};

/// Flag/env (clap) beat the config file; clap's default is last resort when the file is silent.
///
/// **Levels 1–3 are clap's** — a flag beats the process environment, and `.env` reaches the
/// same parser on the second pass (decision 3). [`ValueSource`] says whether clap answered;
/// the config file is the only remaining level and is read only when clap fell back.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::resolve::pick;
///
/// assert_eq!(
///     pick(ValueSource::CommandLine, Some(9001), Some(2)),
///     Some(9001)
/// );
/// assert_eq!(
///     pick(ValueSource::DefaultValue, Some(9000), None),
///     Some(9000)
/// );
/// ```
pub fn pick<T: Clone>(source: ValueSource, parsed: Option<T>, file: Option<T>) -> Option<T> {
    match source {
        ValueSource::CommandLine | ValueSource::EnvVariable => parsed,
        _ => file.or(parsed),
    }
}

/// The chain for a key the config file always has a value for: [`pick`] with the file as
/// last resort.
pub(super) fn chain<T: Clone>(source: ValueSource, parsed: Option<T>, file: T) -> T {
    pick(source, parsed, Some(file.clone())).unwrap_or(file)
}

/// The process's working directory: the walk-up's starting point, for `resolve_root` here and
/// for `service`'s verbs — one body, so `tinio service install` resolves its root from the same
/// place `tinio server` does.
pub(crate) fn current_dir() -> Result<PathBuf, Error> {
    env::current_dir().map_err(Error::from)
}

/// The storage root: the explicit `DIR` when there is one, else the nearest ancestor of `from`
/// that contains a `.tinio` directory, else a usage error naming the remedy.
///
/// The answer is canonical: a root's identity is its canonical path (that is what the
/// home-state-dir hash and the socket name are derived from), so `./data` and `/abs/data` are
/// one root and not two.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::resolve_root;
///
/// let root = resolve_root(Some(Path::new(".")), Path::new(".")).expect("the current directory");
/// assert!(root.is_absolute());
/// ```
pub fn resolve_root(explicit: Option<&Path>, from: &Path) -> Result<PathBuf, Error> {
    let candidate = match explicit {
        Some(dir) => dir.to_path_buf(),
        None => match from
            .ancestors()
            .find(|dir| dir.join(RESERVED_SEGMENT).is_dir())
        {
            Some(dir) => dir.to_path_buf(),
            None => {
                return Err(Error::Usage(format!(
                    "no storage root: pass DIR, or run from a directory inside one — walked up from `{}` looking for a `{RESERVED_SEGMENT}` directory",
                    from.display()
                )));
            }
        },
    };
    candidate.canonicalize().map_err(|err| {
        Error::Usage(format!(
            "the storage root `{}` is not usable: {err} — pass an existing DIR",
            candidate.display()
        ))
    })
}

/// The mode (decision 4): `--read-only` and `TINIO_READ_ONLY` — the flag's two channels, which
/// clap resolves as one answer — then the config file's `[server] read_only`.
///
/// `source` is that answer's [`ValueSource`], and it is what makes the two upper levels
/// *answers* rather than defaults: `TINIO_READ_ONLY=0` with a file saying `read_only = true`
/// resolves read-write, while nothing having answered at all leaves the file's key in place —
/// which a plain `||` over a `bool` could not express.
///
/// **`.env` is deliberately not an input**: the `.env` file's own path is a function of the
/// mode (`<root>/.tinio/.env` normally, the relocated state dir in read-only mode), so letting
/// it decide the mode would close a cycle. That is why this is called from [`locate`] — before
/// the load — with the discovery parse's answers, and why a `TINIO_READ_ONLY` line inside
/// `.env` is inert.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::resolve::resolve_mode;
/// use tinio_config::Config;
///
/// let config = Config::default();
/// assert!(
///     resolve_mode(&config, ValueSource::CommandLine, true),
///     "the flag wins"
/// );
/// assert!(
///     !resolve_mode(&config, ValueSource::EnvVariable, false),
///     "an explicit variable beats the file"
/// );
/// assert!(
///     !resolve_mode(&config, ValueSource::DefaultValue, false),
///     "nothing answered: the config file is the last level"
/// );
/// ```
pub fn resolve_mode(config: &Config, source: ValueSource, read_only: bool) -> bool {
    chain(source, Some(read_only), config.server.read_only)
}

/// Where the tool's private state lives for `root` (FR-023, decision 6).
///
/// Read-write it is the reserved `<root>/.tinio`; read-only it is `override_dir` — the
/// discovery parse's `TINIO_STATE_DIR` — when that is set, else
/// `<home>/.tinio/roots/<sha256(canonical root)16>/`. **Nothing is created here, in either
/// branch**: the derivation is the non-creating [`state::home_state_dir_path`], so a client that
/// only probes leaves the home tree alone. Serving and `--fix` call
/// [`state::prepare_home_derived`] immediately before their `create_dir_all`, which is what
/// restores the 0700 / no-symlink parents `ensure_roots` owns. The home derivation is reached
/// only in read-only mode, which is what keeps a read-write run out of `dirs::home_dir()`.
///
/// **The override is a parameter, never a lookup**: the variable is read once, by the
/// discovery parse, because the file this directory holds is what the second parse has to see.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::resolve_state_dir;
///
/// // The default needs no home directory and no environment.
/// let state = resolve_state_dir(Path::new("/srv/data"), false, None).expect("state dir");
/// assert!(state.ends_with(".tinio"));
/// // Exported: the read-only state dir is the variable, verbatim.
/// let state = resolve_state_dir(Path::new("/srv/data"), true, Some(Path::new("/srv/state")))
///     .expect("state dir");
/// assert_eq!(state, Path::new("/srv/state"));
/// ```
pub fn resolve_state_dir(
    root: &Path,
    read_only: bool,
    override_dir: Option<&Path>,
) -> Result<PathBuf, Error> {
    if !read_only {
        return Ok(root.join(RESERVED_SEGMENT));
    }
    match override_dir {
        Some(dir) => Ok(dir.to_path_buf()),
        // The **non-creating** derivation (V5/R28): resolving a read-only run's state dir must not
        // mint `<home>/.tinio/roots/` merely because a client asked where it would be.
        None => Ok(state::default_home_state_dir_path(root)?),
    }
}

/// The configuration file this run reads: the explicit `--config` when it was given — which
/// must name an existing file, exit 2 otherwise, because a mistyped path must never silently
/// become a new config in a new place — else discovery's own order
/// ([`discover::resolve_path`]).
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::config_path;
///
/// // Nothing exists yet: discovery names the file a first start would create.
/// let path =
///     config_path(Path::new("/srv/data"), Path::new("/srv/data/.tinio"), None).expect("path");
/// assert!(path.ends_with(".tinio.toml"));
/// ```
pub fn config_path(
    root: &Path,
    state_dir: &Path,
    explicit: Option<&Path>,
) -> Result<PathBuf, Error> {
    if let Some(path) = explicit
        && !path.is_file()
    {
        return Err(Error::Usage(format!(
            "the configuration file `{}` does not exist: `--config` names a file that must be there (a first start auto-creates `{}` under the state dir)",
            path.display(),
            discover::CONFIG_FILE
        )));
    }
    discover::resolve_path(root, state_dir, explicit).map_err(Error::from)
}

/// The ordered pair of state directories a client probes (decision 19): the resolved one
/// first, then the **other** candidate.
///
/// The other candidate is `<root>/.tinio` for a read-only run and the home-derived state dir
/// for a read-write one, and `override_dir` (`TINIO_STATE_DIR`, as the caller read it)
/// replaces the home-derived candidate in both modes — but **never** the read-only pair's
/// `<root>/.tinio`, because a read-only run's resolved dir already *is* the variable when it
/// is set: the pair would collapse onto one directory, and the read-write server a read-only
/// client is looking for would be unreachable.
///
/// `derived` is lazy so that nothing touches a home directory when the override answers, and
/// it is not called at all in a read-only run, where the other candidate is a join.
///
/// # Examples
///
/// ```rust
/// use std::path::{Path, PathBuf};
///
/// use tinio_cli::resolve::state_dir_candidates;
///
/// let root = Path::new("/srv/data");
/// let read_only = Path::new("/home/svc/.tinio/roots/0123456789abcdef");
/// let candidates = state_dir_candidates(root, read_only, true, None, || {
///     unreachable!("a read-only pair derives nothing")
/// })
/// .expect("candidates");
/// assert_eq!(candidates, [read_only.to_path_buf(), root.join(".tinio")]);
/// ```
pub fn state_dir_candidates(
    root: &Path,
    resolved: &Path,
    read_only: bool,
    override_dir: Option<PathBuf>,
    derived: impl FnOnce() -> Result<PathBuf, Error>,
) -> Result<Vec<PathBuf>, Error> {
    let other = if read_only {
        root.join(RESERVED_SEGMENT)
    } else {
        match override_dir {
            Some(dir) => dir,
            None => derived()?,
        }
    };
    Ok(vec![resolved.to_path_buf(), other])
}

/// Steps a–d of the chain (design §1), as the **discovery parse** alone can answer them: the
/// root, the mode and the state dir.
///
/// These three are one group because the state dir is a function of the other two, and because
/// they are exactly what decides where `<state dir>/.env` is — which is why they are resolved
/// before the file is loaded rather than after it (the second parse sees the file; this one must
/// not, and decision 4 is the mode's half of that rule).
///
/// # Errors
///
/// Whatever [`resolve_root`] refuses (a root that cannot be canonicalized is a usage error), plus
/// the config failures a root-side configuration file can have while the mode is being read.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     resolve::{Located, locate},
/// };
///
/// // The current directory is the root here, as it is for `resolve_root`'s own leg: a root has
/// // to exist to be canonicalized.
/// let discovery = Discovery {
///     dir: Some(".".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located: Located = locate(discovery, &Provenance::default()).expect("a root");
/// assert!(located.state_dir.ends_with(".tinio"));
/// ```
pub fn locate(discovery: Discovery, provenance: &Provenance) -> Result<Located, Error> {
    locate_with(discovery, provenance, None)
}

/// [`locate`] for a process a confinement launched: the configuration **text** the payload
/// carries stands in for the file an explicit `--config` named (design decision 18).
///
/// **The path is neither re-validated nor read.** The launcher read that file, resolved the mode
/// from it and forwarded its text; inside the confinement the path may sit outside every
/// exposure, so a child that asked for it again would be refused by its own jail — which is
/// exactly how a confined run with `--config` outside the served root used to die before it
/// served anything, with [`config_path`]'s "must be there" usage error. The mode's file level is
/// the text instead, which *is* that file's content, so the child and the launcher that resolved
/// it cannot disagree about the mode.
///
/// # Errors
///
/// Whatever [`resolve_root`] refuses (a root that cannot be canonicalized is a usage error).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     resolve::locate_confined,
/// };
///
/// // The current directory is the root here, as it is for `locate`'s own leg: a root has to
/// // exist to be canonicalized.
/// let discovery = Discovery {
///     dir: Some(".".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located =
///     locate_confined(discovery, &Provenance::default(), "version = 1\n").expect("a root");
/// assert!(!located.read_only);
/// ```
pub fn locate_confined(
    discovery: Discovery,
    provenance: &Provenance,
    config_text: &str,
) -> Result<Located, Error> {
    locate_with(discovery, provenance, Some(config_text))
}

/// [`locate`] and [`locate_confined`]'s one body: the mode's file level is the only thing the
/// payload changes.
fn locate_with(
    discovery: Discovery,
    provenance: &Provenance,
    adopted: Option<&str>,
) -> Result<Located, Error> {
    let root = resolve_root(discovery.dir.as_deref(), &current_dir()?)?;
    // The config the **mode** is read from, before the state dir is known: `<root>/.tinio`'s own
    // pair, which is readable whatever the mode turns out to be (the root-side file is the one
    // FR-023 keeps readable in read-only mode too). A confined child holding the payload's text
    // is the one caller whose file level is that text rather than a path — `mode_config` states
    // the two shapes and why neither of them reads `--config` for such a process.
    let file_mode = super::mode_config(
        &root,
        &root.join(RESERVED_SEGMENT),
        discovery.config.as_deref(),
        adopted,
    )?;
    let read_only = resolve_mode(
        file_mode.as_ref().unwrap_or(&Config::default()),
        provenance.read_only,
        discovery.read_only,
    );
    let state_dir = resolve_state_dir(&root, read_only, discovery.state_dir.as_deref())?;
    Ok(Located {
        root,
        read_only,
        state_dir,
        config: discovery.config,
        state_dir_override: discovery.state_dir,
    })
}

/// The discovery answers the rest of the chain runs on, plus the two it must remember.
///
/// It exists because the chain is one order and the parse is two: everything here is resolved
/// *before* `<state dir>/.env` is loaded, and everything the second parse then adds — the flags,
/// the variables, `.env`'s values among them — is folded on top of it by [`super::server()`] and
/// [`super::for_doctor()`].
#[derive(Debug)]
pub struct Located {
    /// The served storage root, canonical (FR-020's walk-up included).
    pub root: PathBuf,
    /// The resolved mode (decision 4), from the discovery parse's two channels and the root-side
    /// config file.
    pub read_only: bool,
    /// The resolved state directory — where `.env` and the configuration live.
    pub state_dir: PathBuf,
    /// The explicit `--config`, which must name an existing file (`server`'s alone).
    pub config: Option<PathBuf>,
    /// `TINIO_STATE_DIR` as the discovery parse read it, kept for the **candidate pair** a client
    /// probes: it replaces the home-derived candidate in either mode
    /// ([`state_dir_candidates`]), and that answer must be the same one `.env`'s own location
    /// was resolved from — read once, before the load, or the two could disagree.
    pub state_dir_override: Option<PathBuf>,
}

/// Loads `<state dir>/.env` (FR-016's level 3, design §1 step e).
///
/// dotenvy never overrides a variable that is already set, which is exactly what puts `.env`
/// below the process environment without a precedence rule of its own — and it is called
/// **between the two parses**, so the values it adds are read by the second one.
///
/// # Examples
///
/// ```rust
/// use std::env::temp_dir;
///
/// use tinio_cli::resolve::apply_env_file;
///
/// // No `.env` in the temp dir: the call succeeds silently.
/// assert!(apply_env_file(&temp_dir()).is_ok());
/// ```
pub fn apply_env_file(state_dir: &Path) -> Result<(), Error> {
    sources::load_env_file(state_dir).map_err(Error::from)
}
