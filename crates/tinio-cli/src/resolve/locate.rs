//! Root, mode and state-dir location (FR-016 steps a-d).

use std::{
    env,
    path::{Path, PathBuf},
};

use clap::parser::ValueSource;

use super::pg;
use crate::{
    _core::object::RESERVED_SEGMENT,
    _util::state,
    Error,
    cli::{Discovery, Provenance},
    config::{Config, discover, sources, storage},
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
        None => nearest_root(from).ok_or_else(|| no_root(from))?,
    };
    canonical_root(&candidate)
}

/// The nearest ancestor of `from` that holds a `{RESERVED_SEGMENT}` directory, or `None` — the
/// walk-up itself, split out of [`resolve_root`] so [`locate`] can tell "there is no root" (where
/// a pg dsn answers with the synthetic root it derives, ruling R8) from "a root that cannot be
/// used" (which stays an error whatever else the run carries).
fn nearest_root(from: &Path) -> Option<PathBuf> {
    from.ancestors()
        .find(|dir| dir.join(RESERVED_SEGMENT).is_dir())
        .map(Path::to_path_buf)
}

/// A root as its own identity: canonical, because that is what the home-state-dir hash and the
/// socket name are derived from — `./data` and `/abs/data` are one root and not two (FR-020).
fn canonical_root(candidate: &Path) -> Result<PathBuf, Error> {
    candidate.canonicalize().map_err(|err| {
        Error::Usage(format!(
            "the storage root `{}` is not usable: {err} — pass an existing DIR",
            candidate.display()
        ))
    })
}

/// The usage error a run that named no `DIR` and found no `{RESERVED_SEGMENT}` above it answers —
/// the one message both [`resolve_root`] and [`rooted`] produce it with.
fn no_root(from: &Path) -> Error {
    Error::Usage(format!(
        "no storage root: pass DIR, or run from a directory inside one — walked up from `{}` looking for a `{RESERVED_SEGMENT}` directory",
        from.display()
    ))
}

/// The root a run resolves, or `None` when there is none **and** a pg dsn is addressed — the one
/// case [`locate_in`] answers with the synthetic root rather than an error (ruling R8).
///
/// **`current_dir()` is consulted here and nowhere else.** An explicit `DIR` never reads it, and
/// neither does a caller that supplied the walk-up's starting point; **on macOS that lookup is a
/// grant rather than a microsecond**, which is what makes this a correctness rule and not a
/// tidy-up: `env::current_dir()` is `getcwd()`, `getcwd()` there is `fcntl(F_GETPATH)`, and
/// Seatbelt refuses it for a working directory under a directory the profile does not grant — so a
/// confined child **died** with `tinio: I/O error: Operation not permitted (os error 1)` at this
/// call, before it emitted an event, whenever the launcher's working directory sat inside the
/// operator's home (measured on macOS 15.8, 2026-09-25, with `tinio server /tmp/<root> --port 0
/// --sandbox` run from `~/…` and no home rule in the profile). The walk-up shape keeps the lookup,
/// and it does not need a grant of its own: the root the walk finds is an ancestor of the working
/// directory, so the directory is inside the served root's own exposure.
///
/// **The pg arm is the R8 fallback.** With no `DIR` and no `.tinio` above, a run that carries a pg
/// dsn (or the `--pg-dsn` flag, its other channel) is a pg run — and a pg run has no served root to
/// walk up to, its state dir being the dsn-derived synthetic root — so `Ok(None)` says "let the
/// caller resolve that root" instead of the usage error. Without a dsn the error stands exactly as
/// it did, so an ordinary run with neither root nor dsn is unchanged.
///
/// # Errors
///
/// Whatever [`resolve_root`] refuses, plus the working directory's own I/O error.
fn rooted(
    explicit: Option<&Path>,
    from: Option<&Path>,
    pg_dsn: bool,
    adopted: Option<&Path>,
) -> Result<Option<PathBuf>, Error> {
    // **The launcher's answer wins, and it is taken as given rather than re-canonicalized.** A
    // confined child may be refused the call that computes it (the function's own doc, and
    // `tinio_sandbox_core::LaunchPayload::root`), and the value the launcher stated is already
    // the answer: it resolved this root before the child existed and granted its ACE on that
    // spelling. Re-canonicalizing would at best be idle work and at worst be the refusal this
    // parameter exists to avoid.
    if let Some(root) = adopted {
        return Ok(Some(root.to_path_buf()));
    }
    if let Some(dir) = explicit {
        // `from` is deliberately unused on this branch: `resolve_root` never reads it, and passing
        // the working directory here would be the lookup this function exists to avoid.
        return Ok(Some(resolve_root(Some(dir), dir)?));
    }
    let from = match from {
        Some(from) => from.to_path_buf(),
        None => current_dir()?,
    };
    match nearest_root(&from) {
        Some(candidate) => Ok(Some(canonical_root(&candidate)?)),
        None if pg_dsn => Ok(None),
        None => Err(no_root(&from)),
    }
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
/// use tinio_cli::{config::Config, resolve::resolve_mode};
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
///     pg_dsn: None,
/// };
/// let located: Located = locate(discovery, &Provenance::default()).expect("a root");
/// assert!(located.state_dir.ends_with(".tinio"));
/// ```
pub fn locate(discovery: Discovery, provenance: &Provenance) -> Result<Located, Error> {
    locate_with(discovery, provenance, None, None)
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
/// §2.9's `root` argument, and the reason it is a parameter rather than a lookup.
///
/// **The root a confined child serves for is the launcher's**, and the launcher states it on the
/// launch frame (design decision 17's channel, the one the configuration text already travels).
/// The child is the confined process, so `Path::canonicalize` is a call its confinement may
/// refuse — on Windows the call asks the volume mount manager for a DOS name, which an
/// AppContainer can be denied with `Access denied` even though its own ACEs allow the path
/// (`tinio_sandbox_core::LaunchPayload::root` carries the observation from this workspace's CI).
/// The launcher is unconfined, resolved the root before the child existed, and granted the ACE on
/// that very spelling, so this value is the canonical one by construction and is **not**
/// canonicalized again.
///
/// `None` — every unconfined run, and every platform whose confinement leaves the call alone —
/// resolves the root from `DIR` or the walk-up exactly as it always has.
///
/// # Errors
///
/// Whatever [`resolve_root`] refuses (a root that cannot be canonicalized is a usage error).
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
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
///     pg_dsn: None,
/// };
/// let located =
///     locate_confined(discovery, &Provenance::default(), "version = 1\n", None).expect("a root");
/// assert!(!located.read_only);
/// let from_the_launcher = Discovery {
///     dir: Some(".".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// assert_eq!(
///     locate_confined(
///         from_the_launcher,
///         &Provenance::default(),
///         "version = 1\n",
///         Some(Path::new("/a/launcher/said/so")),
///     )
///     .expect("the launcher's root is taken as given, not canonicalized")
///     .root,
///     Path::new("/a/launcher/said/so")
/// );
/// ```
pub fn locate_confined(
    discovery: Discovery,
    provenance: &Provenance,
    config_text: &str,
    root: Option<&Path>,
) -> Result<Located, Error> {
    locate_with(discovery, provenance, Some(config_text), root)
}

/// [`locate`] and [`locate_confined`]'s one body: the mode's file level is the only thing the
/// payload changes.
///
/// **The pg arm runs before the root is resolved** (PG5/PG13): a run whose configuration selects
/// `[storage] type = "pg"` has no served root, so its root is synthesized from the dsn and a
/// positional `DIR` — or a discovered `.tinio/` root — is refused rather than ignored. That is
/// why the selector is read from the document the **operator named** (`--config`, or the
/// confined child's payload text) before [`rooted`] runs: it is the one document a pg run can
/// read without a root, and a pg run has none to walk up to.
fn locate_with(
    discovery: Discovery,
    provenance: &Provenance,
    adopted: Option<&str>,
    adopted_root: Option<&Path>,
) -> Result<Located, Error> {
    locate_in(discovery, provenance, adopted, adopted_root, None)
}

/// [`locate`] with the walk-up's **starting directory named** — the `#[cfg(test)]` entry below,
/// and nothing else: the public pair passes `None`, which is what keeps `current_dir()` lazy
/// ([`rooted`]'s doc is the macOS correctness reason for that laziness).
///
/// It exists because one arm of the resolution — a `.tinio/` root **discovered** by the walk-up
/// — is otherwise reachable only by mutating the process's working directory, which no test may
/// do (it races every other test in the binary), and that arm is where PG13's second refusal and
/// the root-side selector both live.
fn locate_in(
    discovery: Discovery,
    provenance: &Provenance,
    adopted: Option<&str>,
    adopted_root: Option<&Path>,
    from: Option<&Path>,
) -> Result<Located, Error> {
    // The document the operator named, read and parsed **once**: it answers the pg selector here,
    // it is the mode's file level below, and it travels on [`Located`] so the chain that builds
    // the [`super::Resolution`] reuses it rather than reading the file a second and third time. A
    // document that is absent answers `None`; one that is present but does not parse fails the run
    // here ([`anchor_config`]'s reason), before the root is resolved.
    let anchor = anchor_config(&discovery, adopted)?;
    if let Some(config) = anchor.as_ref().map(Anchor::config)
        && config.backend() == storage::Backend::Pg
    {
        let mut located = pg_located(discovery, provenance, config)?;
        located.config_doc = anchor.and_then(Anchor::reusable);
        return Ok(located);
    }
    // The root, or — when the walk-up finds nothing and a pg dsn is addressed — the synthetic root
    // a pg run lives under (ruling R8): `TINIO_PG_DSN=… tinio doctor` with no `DIR` must diagnose
    // the database, and a pg run has no served root for the walk-up to find. `rooted` returns
    // `None` for exactly that arm and nothing else; with no dsn it returns the usage error, so the
    // no-root-with-no-dsn case is unchanged.
    // **A tier document is not a dsn-addressed pg run** (T1, ruling R8). It carries
    // `[storage.pg]` because that section *is* the tier layer's cold plane, so the dsn it names —
    // or the one `TINIO_PG_DSN` exports beside it — must not buy the synthetic root a pg run lives
    // under. With no `DIR` and no discovered root the run has no served root, which is the usage
    // error `rooted` answers rather than a tool-state leaf handed to the serving half as if it
    // were the root to serve.
    let pg_addressed = discovery.pg_dsn.is_some()
        && !anchor
            .as_ref()
            .is_some_and(|anchor| anchor.config().backend() == storage::Backend::Tier);
    let root = match rooted(discovery.dir.as_deref(), from, pg_addressed, adopted_root)? {
        Some(root) => root,
        None => {
            let mut located = pg_located(discovery, provenance, &Config::default())?;
            // **A pg run has exactly one state directory**, so the client pair collapses onto it.
            // §6's read-only rule is "there is no served root and no relocation", which is why the
            // pair's other candidate — `<home>/.tinio/roots/<sha256(canonical root)>/`, a
            // *home-derived* leaf — is a directory this run can never use; and deriving it
            // canonicalizes the root, which a run that just resolved the dsn's synthetic leaf may
            // be the first thing to name (the leaf is created by the serving half, and by nothing
            // the resolution does). The override is what `state_dir_candidates` reads in place of
            // that derivation, so naming one here is what makes a bare `tinio doctor` resolve at
            // all.
            //
            // **It names the root and not the state directory**, which is the value a pg run's
            // override always is: `pg_located` hands `TINIO_STATE_DIR` to `pg::resolve_root` as
            // the root, so an operator-named override already *is* the root here. What this field
            // is **not** is a signal — `Located::pg_root` is the record of which arm ran, and it
            // exists precisely because this one is also the operator's `TINIO_STATE_DIR` on a
            // filesystem run: reading it as "this run is pg" refused a service that had nothing to
            // do with PostgreSQL.
            located.state_dir_override = Some(located.root.clone());
            located.config_doc = anchor.and_then(Anchor::reusable);
            return Ok(located);
        }
    };
    // The config the **mode** is read from, before the state dir is known: `<root>/.tinio`'s own
    // pair, which is readable whatever the mode turns out to be (the root-side file is the one
    // FR-023 keeps readable in read-only mode too). A confined child holding the payload's text
    // is the one caller whose file level is that text rather than a path — `mode_config` states
    // the two shapes and why neither of them reads `--config` for such a process. The anchoring
    // document, when there is one, is its file level instead of a second read of the same file;
    // the pair it answers with is also the document the chain that builds the [`super::Resolution`]
    // reuses, so the file is parsed once for the whole start.
    let file_mode = super::mode_config(
        &root,
        &root.join(RESERVED_SEGMENT),
        discovery.config.as_deref(),
        adopted,
        anchor.as_ref(),
    )?;
    // The same selector through the other door (PG13): a root did resolve — positionally or by
    // the walk-up — and the document under it says pg. The run is refused **here**, where the
    // type is known and the root is in hand, because `resolve_root` cannot know either.
    if let Some((config, _)) = file_mode.as_ref()
        && config.backend() == storage::Backend::Pg
    {
        // The remedy names a channel **this run's command really has**: `--config` is the
        // server arm's flag alone, and the clients (`doctor`, `status`, `stop`, the `service`
        // verbs) build a discovery with none — their channels are the environment's
        // (`pg::refuse`'s doc).
        let names_config = discovery.config.is_some();
        return Err(match discovery.dir.as_deref() {
            Some(dir) => pg::refuse_positional_dir(dir, names_config),
            None => pg::refuse_discovered_root(&root, names_config),
        });
    }
    let read_only = resolve_mode(
        file_mode
            .as_ref()
            .map_or(&Config::default(), |(config, _)| config),
        provenance.read_only,
        discovery.read_only,
    );
    let state_dir = resolve_state_dir(&root, read_only, discovery.state_dir.as_deref())?;
    Ok(Located {
        root,
        read_only,
        state_dir,
        // The filesystem arm ran, so this is not a pg run whatever else the run carries.
        pg_root: None,
        config: discovery.config,
        state_dir_override: discovery.state_dir,
        config_doc: file_mode,
    })
}

/// [`locate`] for a test: the walk-up starts at `from` instead of the process's working
/// directory.
///
/// **Test support, and it is `#[cfg(test)]` for a reason**: the discovered-root arm
/// (`<root>/.tinio/…`) has no other deterministic entry — the production path reads
/// `env::current_dir()`, and a test that changed it would race every other test in this binary —
/// so the seam names the one input instead. Nothing about the resolution differs: the body is
/// [`locate_in`]'s, and only the walk-up's starting point is supplied.
///
/// # Errors
///
/// Whatever [`locate`] refuses, including PG13's refusal for a discovered root under
/// `[storage] type = "pg"`.
#[cfg(test)]
pub(crate) fn locate_from(
    discovery: Discovery,
    provenance: &Provenance,
    from: &Path,
) -> Result<Located, Error> {
    locate_in(discovery, provenance, None, None, Some(from))
}

/// The configuration an operator **named** — `--config`, or the payload text a confinement
/// launched this process with — parsed for the one question that must be answerable before the
/// root exists: does this run select the pg backend?
///
/// A document that is **absent** answers `None`: it can name no backend, and its absence is
/// [`config_path`]'s usage error to report where the root is in hand. A document that is present
/// but **unreadable or unparseable propagates the error** rather than being read as "no backend":
/// swallowing it here made `tinio server --config <broken>` — with no `DIR` and no `.tinio` above
/// the working directory — answer [`rooted`]'s storage-root usage error first, so the operator was
/// sent to look for a root when the configuration was what they had to fix. The error is
/// [`super::document`]'s own, so it is the same one the load chain would have raised later, at the
/// same message; a `--config` that parses still answers `Some`, exactly as before.
///
/// **The document is read once** ([`super::document`], production's only parse): the same pair
/// `mode_config` and the chain that builds the [`super::Resolution`] would have read again travels
/// on [`Located`].
fn anchor_config(discovery: &Discovery, adopted: Option<&str>) -> Result<Option<Anchor>, Error> {
    let Some(path) = discovery.config.as_deref() else {
        return Ok(None);
    };
    match adopted {
        // Exempt text parse: the launcher forwarded this document's bytes unparsed and the
        // confinement exposes no path to read (design decision 18), so there is nothing for
        // `Config::load` to open here — this text came from outside the process, not from code.
        // `mode_config` and `resume` each parse the payload's own text, which is the document
        // (nothing is read twice, and each exemption stands), so the anchor is the parsed
        // configuration alone.
        Some(text) => Ok(Config::parse(text).ok().map(Anchor::Payload)),
        None if path.is_file() => {
            let (config, text) = super::document(path)?;
            Ok(Some(Anchor::Reusable { config, text }))
        }
        None => Ok(None),
    }
}

/// The document an operator **named**, with its own reusable text when it has one.
///
/// The two shapes are the parser's, not this module's: a file was loaded by [`super::document`]
/// (production's only parse), so the text it was parsed from is the one the later legs reuse, while
/// a launcher's payload text (design decision 18) is parsed again by those legs themselves and has
/// no second representation to carry.
pub(super) enum Anchor {
    /// A file's document: the configuration, and the text it was loaded from.
    Reusable { config: Config, text: String },
    /// A launcher's payload text: the configuration it parsed to, and no text of its own.
    Payload(Config),
}

impl Anchor {
    /// The configuration the anchor parsed, whichever shape it came in.
    fn config(&self) -> &Config {
        match self {
            Self::Reusable { config, .. } | Self::Payload(config) => config,
        }
    }

    /// The reusable document [`Located::config_doc`] carries, when this anchor has one.
    fn reusable(self) -> Option<(Config, String)> {
        match self {
            Self::Reusable { config, text } => Some((config, text)),
            Self::Payload(_) => None,
        }
    }
}

/// The [`Located`] a pg run resolves to (PG5/PG13): the synthetic root, the mode, and the state
/// directory under that root.
///
/// **The mode's file level is the anchoring document** ([`anchor_config`]) when there is one,
/// which is the run's own configuration — the pg arm exists because that document said `pg`.
/// [`rooted`]'s R8 fallback is the other caller and has no document at all (there is no root to
/// find one under), so it passes `Config::default()`: the mode is then the flag and the
/// environment alone, which is exactly what a run with no configuration file has. Nothing is lost
/// either way — the document, when there is one, is loaded later by the chain that builds the
/// [`super::Resolution`] from this value, which is where the pool size and the section's own dsn
/// are read.
///
/// **The state directory never relocates for pg**, whatever the mode: §6's read-only rule is
/// "there is no served root and no relocation", because the thing the fs relocation protects —
/// the served root's private state — does not exist here. The root this run resolves is tool
/// state by construction, so `<root>/.tinio` is the answer in both modes and the read-only
/// machine's home-derived (`<home>/.tinio/roots/<sha256(root)16>/`) branch is deliberately not
/// reached (it would also have to canonicalize a root the first run has not created yet).
fn pg_located(
    discovery: Discovery,
    provenance: &Provenance,
    config: &Config,
) -> Result<Located, Error> {
    if let Some(dir) = discovery.dir.as_deref() {
        // The operator named a `--config` — a flag only the server arm carries — so this run's
        // remedy may name one too (`pg::refuse`).
        return Err(pg::refuse_positional_dir(dir, discovery.config.is_some()));
    }
    let dsn = pg::resolve_dsn(config.pg_section(), discovery.pg_dsn.as_ref())?;
    let root = pg::resolve_root(&dsn, discovery.state_dir.as_deref())?;
    let read_only = resolve_mode(config, provenance.read_only, discovery.read_only);
    let state_dir = resolve_state_dir(&root, false, discovery.state_dir.as_deref())?;
    Ok(Located {
        root: root.clone(),
        read_only,
        state_dir,
        // This is the arm that ran, so the root it derived is recorded as the pg run's own.
        pg_root: Some(root),
        config: discovery.config,
        state_dir_override: discovery.state_dir,
        // The caller threads the anchoring document onto the value it returns (`locate_in`), so the
        // chain that builds the `Resolution` reuses the one read rather than a second.
        config_doc: None,
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
    /// The served storage root, canonical (FR-020's walk-up included) — or, for a pg run, the
    /// **synthetic** root its configuration and tool state live under ([`super::pg::resolve_root`]).
    pub root: PathBuf,
    /// The resolved mode (decision 4), from the discovery parse's two channels and the root-side
    /// config file.
    pub read_only: bool,
    /// The resolved state directory — where `.env` and the configuration live.
    pub state_dir: PathBuf,
    /// The explicit `--config`, which must name an existing file (`server`'s alone).
    pub config: Option<PathBuf>,
    /// The synthetic root the **pg arm** derived, when that is the arm this resolution took
    /// (PG5/PG13): `Some(root)` for a pg run, `None` for every filesystem one — `TINIO_PG_DSN`
    /// exported included, which is the whole point of recording it here rather than re-deriving
    /// it downstream.
    ///
    /// **It exists so "this run is a pg one" is a fact rather than an inference.** Every other
    /// field a consumer could test is ambiguous: `state_dir_override`, for instance, is the
    /// operator's `TINIO_STATE_DIR` on a filesystem run and the derived root on the dsn-only one,
    /// so a run that set the variable to its own root would read as pg. The commands that have to
    /// tell the two apart — the serving half's refusal and the doctor's check list, both through
    /// `commands::is_pg_root` — read this field.
    pub pg_root: Option<PathBuf>,
    /// `TINIO_STATE_DIR` as the discovery parse read it, kept for the **candidate pair** a client
    /// probes: it replaces the home-derived candidate in either mode
    /// ([`state_dir_candidates`]), and that answer must be the same one `.env`'s own location
    /// was resolved from — read once, before the load, or the two could disagree. **It is copied
    /// onto the [`super::Resolution`] this value is consumed by**, which is where the two readers
    /// that would otherwise have to take it off `Located` *before* the move find it: the
    /// confinement's read-only macOS rule (which reads whether it is there) and
    /// [`super::state_dir_candidates`]'s caller.
    pub state_dir_override: Option<PathBuf>,
    /// The document the mode was resolved from — the operator's `--config`, or `<root>/.tinio`'s
    /// own file — **read and parsed once**, as its `Config` beside the text it was parsed from.
    /// The chain that builds the [`super::Resolution`] reuses the pair instead of reading the file
    /// again, so a start parses its configuration once rather than twice, whether `--config` named
    /// it or not.
    ///
    /// It is `None` when no document was read at all — no `--config` and no root-side file (which
    /// the chain's read or auto-create answers), a file that does not parse (the chain's read
    /// reports the error), and a launcher's payload text: the two legs that read that text parse
    /// it themselves (design decision 18), so nothing about their exempt `Config::parse` changes.
    pub config_doc: Option<(Config, String)>,
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
