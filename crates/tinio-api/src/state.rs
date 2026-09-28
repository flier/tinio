//! The management plane's `state` file, and the token that guards `/status` and `/stop`.
//!
//! The file lives in the state directory the caller resolved (design decision 6), so
//! read-only mode's relocation needs no code here. Its presence means "a server claims
//! this root"; the effective single-instance lock is the control channel itself
//! (`crate::bind`), and this file is how `status` and `stop` find that
//! channel — `control_name` is the source of truth for it, so no reader re-derives a socket
//! or pipe name. The unix socket's default name is derived here too (design decision 24),
//! from the same root the Windows pipe's name comes from.

use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    process,
};
#[cfg(unix)]
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub use crate::_util::secret::Secret;
#[cfg(windows)]
use crate::_winsec::{acl_is_current_user_only, create_restricted, restrict_to_current_user};
use crate::{_util::secret::SecretKey, Error};

/// The file's name inside the state directory.
///
/// # Examples
///
/// ```rust
/// use tinio_api::state::STATE_FILE;
///
/// assert_eq!(STATE_FILE, "state");
/// ```
pub const STATE_FILE: &str = "state";

/// The schema version this build writes and the only one it reads.
///
/// # Examples
///
/// ```rust
/// use tinio_api::state::STATE_VERSION;
///
/// assert_eq!(STATE_VERSION, 1);
/// ```
pub const STATE_VERSION: u32 = 1;

/// The token's length in bytes, before hex encoding (`management-api.md`: ≥ 32).
const TOKEN_BYTES: usize = 32;

/// The unix control socket's **default** path: `tinio-<sha1(canonicalize(root))[..16]>.sock`
/// in `state_dir` (design decision 24).
///
/// Derived from the root exactly like the Windows pipe's name, so two roots sharing one
/// state dir — the deployment `TINIO_STATE_DIR` exists to allow — cannot collide on the
/// socket path. `[api.unix] path` (relative or absolute) overrides it; whichever wins, the
/// caller records its **resolved absolute** path in [`State::control_name`], and a reader of
/// `state` never re-derives the name.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::state::socket_path;
///
/// let socket = socket_path(Path::new("/run/tinio"), Path::new(".")).expect("socket path");
/// let name = socket.file_name().expect("file name").to_string_lossy();
/// assert!(name.starts_with("tinio-"), "{name}");
/// assert!(name.ends_with(".sock"), "{name}");
/// ```
pub fn socket_path(state_dir: &Path, root: &Path) -> Result<PathBuf, Error> {
    Ok(state_dir.join(format!("{}.sock", channel_stem(root)?)))
}

/// The Windows named pipe's name: `tinio-<sha1(canonical root)[..16]>`, the contract's form
/// (`contracts/management-api.md`), from the same digest [`socket_path`] derives.
///
/// **A name, not a path.** [`socket_path`] answers with a file inside the state directory;
/// Windows' pipe namespace is flat, so the channel's name carries no directory — and must not,
/// or anyone able to enumerate `\\.\pipe\` reads the server's local layout out of it.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::state::pipe_name;
///
/// let name = pipe_name(Path::new(".")).expect("pipe name");
/// assert!(name.starts_with("tinio-"), "{name}");
/// assert!(!name.ends_with(".sock"), "{name}");
/// ```
pub fn pipe_name(root: &Path) -> Result<String, Error> {
    channel_stem(root)
}

/// The per-root channel stem both forms share: `tinio-<sha1(canonical root)[..16]>`.
fn channel_stem(root: &Path) -> Result<String, Error> {
    let canonical = root.canonicalize().map_err(|err| Error::State {
        path: root.to_path_buf(),
        message: format!("the served root cannot be canonicalized: {err}"),
    })?;
    let digest = hex::encode(Sha1::digest(canonical.as_os_str().as_encoded_bytes()));
    Ok(format!("tinio-{}", &digest[..16]))
}

/// What a running server records about itself.
///
/// **The token is a [`Secret`]** — the shared `SecretBox` layer (`tinio_util::secret`);
/// redacted from `Debug` by construction. This value rides inside the client's `Report`, so
/// any `tracing::debug!(?report)` in a CLI would otherwise print the bearer token into the
/// operational log. Every other field renders as the derive would.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use secrecy::ExposeSecret;
/// use tinio_api::state::State;
///
/// let state = State::new(
///     "/run/tinio/tinio-0123456789abcdef.sock",
///     Some(9000),
///     Path::new("."),
/// )
/// .expect("state");
/// assert_eq!(state.version, 1);
/// assert_eq!(state.port, Some(9000));
/// // The token is readable where it is used — and never through `Debug`.
/// assert_eq!(state.token.expose_secret().len(), 64);
/// let printed = format!("{state:?}");
/// assert!(printed.contains("REDACTED"), "{printed}");
/// assert!(
///     !printed.contains(state.token.expose_secret().as_str()),
///     "{printed}"
/// );
/// ```
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct State {
    /// [`STATE_VERSION`] at the time of writing; a different value is refused on read.
    pub version: u32,
    /// The serving process's pid. Diagnostic only: nothing in this phase checks it for
    /// liveness — an absent control channel, not a stale pid, is what "stopped" means.
    pub pid: u32,
    /// The `X-Tinio-Token` value, hex-encoded `TOKEN_BYTES`.
    ///
    /// [`Secret`]: bearer credential via the shared box; `Debug` redacts it so a log line
    /// that dumps a `State` cannot print the value that guards `/status` and `/stop`.
    pub token: Secret,
    /// The control channel's identity: the socket path (absolute — the derived default or
    /// the `[api.unix] path` override), the pipe name, or the TCP address. Clients read this
    /// and never re-derive it.
    pub control_name: String,
    /// The data plane's bound port (`None` before it is bound).
    pub port: Option<u16>,
    /// The served storage root, canonical.
    pub root: PathBuf,
    /// RFC3339, UTC.
    pub started_at: String,
}

/// The `state` file's path inside `state_dir`.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::state::path;
///
/// let state_dir = Path::new("/run/tinio");
/// assert_eq!(path(state_dir), state_dir.join("state"));
/// ```
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(STATE_FILE)
}

/// Removes `path`, treating "it is not there" as success.
///
/// **The tolerance is the point, and it is narrow.** Every caller is *making sure* the path is
/// gone — a stale temp before a fresh create, the `state` file on the way out, the socket file
/// this plane still owns — and a path that is already absent satisfies that intent; absence is
/// also the ordinary case (`Channel::socket_path` is `None` on Windows, and a plane that never
/// reached `publish` has no `state`). Everything else is an error, `NotFound` alone: a permission
/// problem or a directory in the way is not what the caller asked for, and absorbing it would
/// report a removal that did not happen.
pub(crate) fn remove_if_present(path: &Path) -> Result<(), Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Error::Io(err)),
    }
}

/// A fresh token: `TOKEN_BYTES` CSPRNG bytes, hex-encoded.
///
/// # Examples
///
/// ```rust
/// use tinio_api::state::generate_token;
///
/// let token = generate_token().expect("token");
/// assert_eq!(token.len(), 64);
/// ```
pub fn generate_token() -> Result<String, Error> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|err| Error::State {
        path: PathBuf::from("<csprng>"),
        message: format!("could not generate a token: {err}"),
    })?;
    Ok(hex::encode(bytes))
}

impl State {
    /// Records this process as the server for `root`, with a fresh token.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::Path;
    ///
    /// use secrecy::ExposeSecret;
    /// use tinio_api::state::State;
    ///
    /// let state = State::new(
    ///     "/run/tinio/tinio-0123456789abcdef.sock",
    ///     None,
    ///     Path::new("."),
    /// )
    /// .expect("state");
    /// assert_eq!(state.pid, std::process::id());
    /// assert_eq!(state.token.expose_secret().len(), 64);
    /// ```
    pub fn new(
        control_name: impl Into<String>,
        port: Option<u16>,
        root: &Path,
    ) -> Result<Self, Error> {
        Self::with_token(control_name, port, root, generate_token()?)
    }

    /// Records this process as the server for `root`, with **the caller's** token.
    ///
    /// **For the plane that does not own its `state` file.** A confined child on Windows
    /// cannot create or write that file — its DACL is current-user-only and protected, and the
    /// child's identity inside the container is the package SID — so the **launcher** creates
    /// it and generates the token that guards `/status` and `/stop`. The child adopts the same
    /// token here, which is what makes the value a client reads out of that file the value this
    /// plane verifies against: the two would otherwise be two answers to one question, and every
    /// client that found the server would be refused `401` by it.
    ///
    /// `pid` is this process's either way, and it is deliberately **not** what the caller's file
    /// carries: the launcher's file names the launcher, so `Plane::shutdown`'s guard — `state.pid
    /// == process::id()`, the check that keeps a plane from deleting a file it did not write —
    /// holds by construction and the child never attempts a deletion its confinement would refuse.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::Path;
    ///
    /// use secrecy::ExposeSecret;
    /// use tinio_api::state::State;
    ///
    /// let token = "00".repeat(32);
    /// let state = State::with_token(
    ///     "/run/tinio/tinio-0123456789abcdef.sock",
    ///     Some(9000),
    ///     Path::new("."),
    ///     token.clone(),
    /// )
    /// .expect("state");
    /// assert_eq!(state.token.expose_secret().as_str(), token.as_str());
    /// assert_eq!(state.pid, std::process::id());
    /// ```
    pub fn with_token(
        control_name: impl Into<String>,
        port: Option<u16>,
        root: &Path,
        token: impl Into<String>,
    ) -> Result<Self, Error> {
        Ok(Self {
            version: STATE_VERSION,
            pid: process::id(),
            token: SecretKey::from(token.into()).into(),
            control_name: control_name.into(),
            port,
            root: root.to_path_buf(),
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|err| Error::State {
                    path: PathBuf::from("<clock>"),
                    message: format!("could not format the start time: {err}"),
                })?,
        })
    }

    /// Writes the file atomically (a temp file beside it, then a rename), mode 0600 on unix
    /// and a current-user-only DACL on Windows.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use tinio_api::state::State;
    ///
    /// let root = Path::new("/srv/tinio");
    /// let state =
    ///     State::new("/run/tinio/tinio-0123456789abcdef.sock", Some(9000), root).expect("state");
    /// state.write(Path::new("/run/tinio")).expect("write");
    /// ```
    pub fn write(&self, state_dir: &Path) -> Result<(), Error> {
        fs::create_dir_all(state_dir).map_err(Error::Io)?;
        let text = serde_json::to_string_pretty(self).map_err(|err| Error::State {
            path: path(state_dir),
            message: format!("could not encode: {err}"),
        })?;

        let tmp = state_dir.join("state.tmp");
        // A stale temp is removed rather than reused. Both platforms apply the restriction only
        // when they *create* — Windows ignores `SECURITY_ATTRIBUTES`, and unix ignores `mode`, for
        // an object that already exists — so writing into a pre-existing `state.tmp` would leave
        // the token in whatever that object was: a plant carrying the directory's inherited
        // grants keeps them, and a symlink is followed rather than replaced. Removing first makes
        // the path fresh, and the create call then has to make the file itself.
        remove_if_present(&tmp)?;
        // The arms differ in what they hand the create call, not in when the restriction applies:
        // unix's `mode` and Windows' `SECURITY_ATTRIBUTES` are both taken by that call, and both
        // arms refuse to adopt an object that appeared in the meantime (`create_new`/`CREATE_NEW`
        // fail rather than truncate). Creating first and restricting after would leave a window in
        // which this file — the one that holds the token — exists under the directory's inherited
        // ACL, and a reader that opened it in that window keeps the access its handle was granted.
        #[cfg(unix)]
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(Error::Io)?;
        #[cfg(windows)]
        let mut file = create_restricted(&tmp).map_err(Error::Io)?;
        // **Verified rather than re-asserted, and the difference is what a confined run can do.**
        // `create_restricted` already installs the protected current-user-only DACL at creation —
        // `CurrentUserDacl` marks the descriptor `SE_DACL_PROTECTED`, which is what keeps the
        // parent directory's inheritable ACEs out — while installing one again through
        // `SetNamedSecurityInfoW` needs `WRITE_DAC`. A confined child does not hold that right and
        // must not: the launcher grants the state directory `(OI)(CI)M`, whose `M` deliberately
        // excludes it (design §9, decision 14). So the descriptor the create call installed is read
        // back, and repaired only when it did not take. **The promise is unchanged**: nothing is
        // published by the rename unless it is current-user-only, and a descriptor that really
        // failed to take ends in the same refusal rather than in a published token.
        #[cfg(windows)]
        if !acl_is_current_user_only(&tmp).map_err(Error::Io)? {
            restrict_to_current_user(&tmp)?;
        }
        file.write_all(text.as_bytes()).map_err(Error::Io)?;
        file.sync_all().map_err(Error::Io)?;
        fs::rename(&tmp, path(state_dir)).map_err(Error::Io)
    }

    /// Reads the file: `Ok(None)` when it is absent, an error when it is unreadable or was
    /// written by a schema version this build does not read (naming both versions).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use tinio_api::state::State;
    ///
    /// let state = State::read(Path::new("/run/tinio")).expect("read");
    /// assert!(state.is_some());
    /// ```
    pub fn read(state_dir: &Path) -> Result<Option<Self>, Error> {
        let Some(state) = Self::read_any_version(state_dir)? else {
            return Ok(None);
        };
        if state.version != STATE_VERSION {
            // The file's version and this build's, both by name, plus what the operator can
            // actually do about it: `version n is not the m this build reads` is true for an
            // older file and a newer one alike, so the remedy is named rather than guessed.
            return Err(Error::State {
                path: path(state_dir),
                message: format!(
                    "version {} is not the {STATE_VERSION} this build reads: a newer tinio is \
                     needed, or remove this file if no server is running",
                    state.version
                ),
            });
        }
        Ok(Some(state))
    }

    /// [`read`](Self::read) **without the version check**: `Some` whenever a document decoded,
    /// whatever version it names.
    ///
    /// The two questions a caller can ask are not the same one. "Give me a `State` I may act on"
    /// must refuse a foreign version, because a field this build does not know may mean something
    /// it would misread — that is [`read`](Self::read). "What channel did the writer bind?" is
    /// answerable from a document this build cannot otherwise use, and it is the question a probe
    /// **has** to ask: the recorded `control_name` is the only route to a channel whose file a
    /// *newer* server wrote, and without it a caller dials the derived name instead, misses a
    /// server that recorded an override, and concludes the channel is silent.
    pub fn read_any_version(state_dir: &Path) -> Result<Option<Self>, Error> {
        let path = path(state_dir);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::Io(err)),
        };
        let state: Self = serde_json::from_str(&text).map_err(|err| Error::State {
            path,
            message: format!("could not decode: {err}"),
        })?;
        Ok(Some(state))
    }

    /// Removes the file; absent is success.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use tinio_api::state::State;
    ///
    /// State::remove(Path::new("/run/tinio")).expect("remove");
    /// ```
    pub fn remove(state_dir: &Path) -> Result<(), Error> {
        remove_if_present(&path(state_dir))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    fn dir() -> TempDir {
        TempDir::new().expect("temp dir")
    }

    /// The name a run records: the derived absolute socket path (design decision 24) —
    /// there is no fixed `tinio.sock` default to fall back on.
    fn control(state_dir: &Path, root: &Path) -> String {
        socket_path(state_dir, root)
            .expect("socket path")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn round_trips_through_the_file() {
        let (dir, root) = (dir(), dir());
        let name = control(dir.path(), root.path());
        let state = State::new(&name, Some(9000), root.path()).expect("new");
        state.write(dir.path()).expect("write");

        let read = State::read(dir.path()).expect("read").expect("present");
        assert_eq!(read.version, STATE_VERSION);
        assert_eq!(read.pid, process::id());
        assert_eq!(read.token, state.token);
        assert_eq!(read.control_name, name);
        assert_eq!(read.port, Some(9000));
        assert_eq!(read.root, root.path());
        assert_eq!(read.started_at, state.started_at);
    }

    #[test]
    fn the_socket_name_is_derived_from_the_root() {
        let state_dir = dir();
        let (a, b) = (dir(), dir());

        let first = socket_path(state_dir.path(), a.path()).expect("a");
        let second = socket_path(state_dir.path(), b.path()).expect("b");
        assert_ne!(
            first, second,
            "two roots sharing one state dir must not collide"
        );
        assert_eq!(
            first.parent(),
            Some(state_dir.path()),
            "the socket lives in the state dir"
        );
        let name = first.file_name().expect("name").to_string_lossy();
        assert!(
            name.starts_with("tinio-") && name.ends_with(".sock"),
            "{name}"
        );
        assert_eq!(
            name.len(),
            "tinio-".len() + 16 + ".sock".len(),
            "the hash prefix is 16 hex characters: {name}"
        );
        assert!(
            first.is_absolute(),
            "the recorded control name is absolute: {}",
            first.display()
        );

        // The name is a function of the *canonical* root: any spelling of the same root
        // derives the same socket, and a root that cannot be canonicalized is an error
        // rather than a silently different name.
        let leaf = a.path().file_name().expect("leaf");
        let respelled = a.path().join("..").join(leaf);
        assert_eq!(
            socket_path(state_dir.path(), &respelled).expect("respelled"),
            first
        );
        assert!(socket_path(state_dir.path(), Path::new("/no/such/root")).is_err());
    }

    #[test]
    fn an_absent_file_is_none_rather_than_an_error() {
        let dir = dir();
        assert!(State::read(dir.path()).expect("read").is_none());
    }

    #[test]
    fn a_malformed_file_is_an_error_naming_the_path() {
        let dir = dir();
        fs::write(path(dir.path()), b"{not json").expect("seed");
        let err = State::read(dir.path()).expect_err("malformed");
        assert!(format!("{err}").contains("state"), "names the file: {err}");
    }

    #[test]
    fn an_unknown_version_is_refused_naming_both_versions() {
        let (dir, root) = (dir(), dir());
        let mut state =
            State::new(control(dir.path(), root.path()), None, root.path()).expect("new");
        state.version = STATE_VERSION + 1;
        state.write(dir.path()).expect("write");

        let err = State::read(dir.path()).expect_err("a future version is not ours to read");
        let text = format!("{err}");
        assert!(
            text.contains(&(STATE_VERSION + 1).to_string()),
            "names the file's version: {text}"
        );
        assert!(
            text.contains(&STATE_VERSION.to_string()),
            "names this build's version: {text}"
        );
        assert!(
            text.contains("newer"),
            "names the remedy for a newer file: {text}"
        );
        assert!(
            text.contains("remove"),
            "names the way out when no server is running: {text}"
        );
    }

    #[test]
    fn tokens_are_long_and_distinct() {
        let a = generate_token().expect("token");
        let b = generate_token().expect("token");
        assert!(
            a.len() >= 64,
            "64 hex characters carry the contract's 32 bytes: {a}"
        );
        assert_ne!(a, b);
    }

    #[test]
    fn remove_is_idempotent() {
        let (dir, root) = (dir(), dir());
        let state = State::new(control(dir.path(), root.path()), None, root.path()).expect("new");
        state.write(dir.path()).expect("write");
        State::remove(dir.path()).expect("remove");
        assert!(State::read(dir.path()).expect("read").is_none());
        State::remove(dir.path()).expect("removing nothing is not an error");
    }

    #[cfg(unix)]
    #[test]
    fn the_file_is_private() {
        let (dir, root) = (dir(), dir());
        State::new(control(dir.path(), root.path()), None, root.path())
            .expect("new")
            .write(dir.path())
            .expect("write");
        let mode = fs::metadata(path(dir.path()))
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the token is in this file");
    }
}
