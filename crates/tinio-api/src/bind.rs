//! Binding the control channel — which is also the single-instance lock.
//!
//! A second process on the same root fails here, before it can touch anything else: the
//! unix socket is unlinked only after a probe proves nobody is listening behind it, and the
//! Windows pipe is created with `FILE_FLAG_FIRST_PIPE_INSTANCE` (`management-api.md:5`).
//!
//! **Nothing here needs a runtime.** The listeners are `std` and the pipe is a raw handle
//! carrying a DACL, because the CLI binds *before* it builds its runtime (design §1: steps a–j,
//! then k) and tokio's own constructors cannot be called there — `tokio::net::UnixListener::bind`
//! panics ("there is no reactor running, must be called from the context of a Tokio 1.x
//! runtime"), and `NamedPipeServer::from_raw_handle`, which both `ServerOptions::create` and
//! `create_with_security_attributes_raw` end in, reaches the same `PollEvented::new`. The
//! conversion into tokio's types is `serve_transport`'s job, inside the runtime.

#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
#[cfg(unix)]
use std::{
    fs,
    os::unix::{
        ffi::OsStrExt,
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
};
use std::{io, path::PathBuf};

use crate::Error;
#[cfg(windows)]
use crate::dacl;

/// A bound control channel, before it is served. This is the unix form: the bound `std` listener
/// and the path it owns.
///
/// The struct owns what it bound, and dropping the channel is what releases the single-instance
/// claim: the socket file is then the stale residue `reclaim_stale` reclaims on the next bind.
/// The fields are crate-visible and no further, because `transport::serve_local` destructures the
/// channel it was handed; [`Channel::identity`] and [`Channel::socket_path`] are what every other
/// caller reads.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(unix)]
/// # {
/// use std::path::Path;
///
/// use tinio_api::bind::bind_unix;
///
/// let channel = bind_unix(Path::new("/run/tinio/tinio-0123456789abcdef.sock")).expect("bind");
/// assert_eq!(channel.identity(), "/run/tinio/tinio-0123456789abcdef.sock");
/// # }
/// ```
#[cfg(unix)]
#[derive(Debug)]
pub struct Channel {
    /// The bound listener — `std`, so the bind needed no runtime.
    pub(crate) listener: UnixListener,
    /// The path the bind created, authored once here and never re-derived.
    pub(crate) path: PathBuf,
}

/// A bound control channel, before it is served. This is the Windows form: the pipe name and the
/// bound first instance the runtime adopts.
///
/// The struct owns what it bound, and dropping the channel is what releases the single-instance
/// claim: the pipe ceases to exist with its last handle. The fields are crate-visible and no
/// further, because `transport::serve_local` destructures the channel it was handed;
/// [`Channel::identity`] is what every other caller reads.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(windows)]
/// # {
/// use tinio_api::bind::bind_pipe;
///
/// // A bare name is normalised to the full form the client dials.
/// let channel = bind_pipe("tinio-mine").expect("bind");
/// assert!(channel.identity().starts_with(r"\\.\pipe\"));
/// # }
/// ```
#[cfg(windows)]
#[derive(Debug)]
pub struct Channel {
    /// The pipe name (`\\.\pipe\tinio-<sha1(root)[..16]>`, or `[api.pipe] path`).
    pub(crate) name: String,
    /// The bound first instance, created with its DACL and adopted inside the runtime
    /// (`dacl::adopt_pipe`, Task 4): an `OwnedHandle` rather than tokio's
    /// `NamedPipeServer`, because creating the latter needs the reactor the CLI does not
    /// have yet, and because it is `Send` where a raw pointer is not.
    pub(crate) handle: OwnedHandle,
}

#[cfg(unix)]
impl Channel {
    /// The identity recorded in `state` and printed by `status`: the bound socket path.
    /// `state.control_name` carries that value — clients read it, never re-derive it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # {
    /// use std::path::Path;
    ///
    /// use tinio_api::bind::bind_unix;
    ///
    /// let channel = bind_unix(Path::new("/run/tinio/tinio-0123456789abcdef.sock")).expect("bind");
    /// assert_eq!(channel.identity(), "/run/tinio/tinio-0123456789abcdef.sock");
    /// # }
    /// ```
    pub fn identity(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    /// The socket path the bind created — **always** `Some` on this platform, where the channel
    /// is a socket file. The method is on both forms because the transport calls it
    /// unconditionally, and the Windows answer `None` is what keeps that call `#[cfg]`-free.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # {
    /// use std::path::Path;
    ///
    /// use tinio_api::bind::bind_unix;
    ///
    /// let path = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
    /// let channel = bind_unix(path).expect("bind");
    /// assert_eq!(channel.socket_path().as_deref(), Some(path));
    /// # }
    /// ```
    pub fn socket_path(&self) -> Option<PathBuf> {
        Some(self.path.clone())
    }
}

#[cfg(windows)]
impl Channel {
    /// The identity recorded in `state` and printed by `status`: the pipe name.
    /// `state.control_name` carries that value — clients read it, never re-derive it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use tinio_api::bind::bind_pipe;
    ///
    /// let channel = bind_pipe("tinio-mine").expect("bind");
    /// assert!(channel.identity().starts_with(r"\\.\pipe\"));
    /// # }
    /// ```
    pub fn identity(&self) -> String {
        self.name.clone()
    }

    /// **Always** `None` on this platform: the Windows channel is a pipe, which owns a name and
    /// not a path. The method is on both forms because the transport calls it unconditionally —
    /// the `None` answered here is what keeps that call `#[cfg]`-free.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use tinio_api::bind::bind_pipe;
    ///
    /// let channel = bind_pipe("tinio-mine").expect("bind");
    /// assert_eq!(channel.socket_path(), None);
    /// # }
    /// ```
    pub fn socket_path(&self) -> Option<PathBuf> {
        None
    }
}

/// The `sun_path` limit **including** the terminating NUL: 108 on Linux, 104 on macOS.
///
/// `pub` because plan C's `doctor` reports the resolved socket path *against* this limit rather
/// than spelling a number of its own — one owner for the platform constant, and the consumer
/// names it instead of a second copy.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::bind::SUN_PATH_MAX;
///
/// let derived = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
/// assert!(derived.as_os_str().len() < SUN_PATH_MAX, "the default fits");
/// ```
#[cfg(unix)]
pub const SUN_PATH_MAX: usize = if cfg!(target_os = "macos") { 104 } else { 108 };

/// The refusal an over-long socket path gets. There is **no fallback** — abstract sockets are
/// not portable to macOS (`research.md:56`'s "documented limitation, no workaround" stands),
/// so the message names the remedy instead: `[api.unix] path` may be absolute (decision 24).
#[cfg(unix)]
fn over_long(path: &Path) -> Error {
    Error::Transport(format!(
        "the socket path {} is too long for `sun_path` ({SUN_PATH_MAX} bytes): set \
         `[api.unix] path` to an absolute path in a short directory",
        path.display()
    ))
}

/// Probes `path` and reclaims it when it is a stale socket file (one left by a
/// crashed instance), leaving a live listener alone. `dry_run` reports what a
/// real call would do without unlinking anything — the same one-code-path-plus-
/// a-flag shape `Cleanup::repair` uses, so `doctor` (plan C) never spells the
/// probe a second time.
///
/// `Ok(true)` — a stale file was removed (dry run: would be). `Ok(false)` —
/// there was nothing to reclaim. `Err` — a live server answered, or the file
/// could not be probed.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::bind::reclaim_stale;
///
/// let path = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
/// // `true` only observes: what a fix would remove is reported, not removed.
/// if reclaim_stale(path, true).expect("probe") {
///     println!("a crashed instance left this socket behind");
/// }
/// ```
#[cfg(unix)]
pub fn reclaim_stale(path: &Path, dry_run: bool) -> Result<bool, Error> {
    if !path.exists() {
        return Ok(false);
    }

    // A listener behind the file means a live server; anything else is residue.
    match UnixStream::connect(path) {
        Ok(_) => Err(Error::Transport(format!(
            "another server is already listening on {}",
            path.display()
        ))),
        Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => {
            if !dry_run {
                fs::remove_file(path).map_err(Error::Io)?;
            }
            Ok(true)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(Error::Transport(format!(
            "{} exists and cannot be probed: {err}",
            path.display()
        ))),
    }
}

/// Binds the unix socket at `path`, reclaiming a stale file left by a crashed instance —
/// [`reclaim_stale`] is that step, and it is the same implementation plan C's `doctor` probes
/// with, so there is one probe and not two.
///
/// The listener is `std` and stays non-blocking, because tokio's own bind cannot be called
/// before the runtime exists; `serve_local` (Task 4) converts it with `from_std` inside the
/// runtime, which is where tokio requires the non-blocking flag to already be set.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::bind::bind_unix;
///
/// let path = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
/// let channel = bind_unix(path).expect("bind");
/// assert_eq!(channel.identity(), path.to_string_lossy());
/// ```
#[cfg(unix)]
pub fn bind_unix(path: &Path) -> Result<Channel, Error> {
    // Checked here as well as at the bind so the refusal names the remedy on every platform,
    // whatever its own limit is; the platform's check below is the authority.
    if path.as_os_str().as_bytes().len() >= SUN_PATH_MAX {
        return Err(over_long(path));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    reclaim_stale(path, false)?;

    let listener = UnixListener::bind(path).map_err(|err| {
        if err.kind() == io::ErrorKind::AddrInUse {
            Error::Transport(format!(
                "another server is already listening on {}",
                path.display()
            ))
        } else if err.kind() == io::ErrorKind::InvalidInput {
            over_long(path)
        } else {
            Error::Transport(format!("could not bind {}: {err}", path.display()))
        }
    })?;
    // Non-blocking, and private, both at the bind: `from_std` inside the runtime refuses a
    // blocking socket, and the mode has to be set here because the file exists only briefly.
    listener.set_nonblocking(true).map_err(Error::Io)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(Error::Io)?;
    Ok(Channel {
        listener,
        path: path.to_path_buf(),
    })
}

/// Binds the named pipe `name`, refusing when another instance already owns it, and keeps the
/// name on the channel (`NamedPipeServer` will not hand it back).
///
/// **The pipe is created raw, with a current-user-only DACL, and this is a deliberate
/// departure from `ServerOptions::create` on both counts.** The DACL is Task 1's reason
/// extended to the channel: `create` passes a null `SECURITY_ATTRIBUTES`, so the pipe gets the
/// creating token's default descriptor (the user, SYSTEM and Administrators full, Everyone and
/// ANONYMOUS LOGON read-only — measured on this host), while the channel is the one place this
/// crate claims is private (`Auth::Local` leaves `/metrics` and `/openapi.json` open on it,
/// decision 8). And the raw call is also what keeps the bind runtime-free: `ServerOptions`
/// ends in `NamedPipeServer::from_raw_handle`, which needs the reactor the CLI does not have
/// yet. `dacl` is the crate's one raw module, so this is an added use of that site, not a
/// second `unsafe` site (Task 1's lint note).
///
/// # Examples
///
/// ```no_run
/// use tinio_api::bind::bind_pipe;
///
/// // A bare name is normalised to the full form the client dials.
/// let channel = bind_pipe("tinio-mine").expect("bind");
/// assert!(channel.identity().starts_with(r"\\.\pipe\"));
/// ```
#[cfg(windows)]
pub fn bind_pipe(name: &str) -> Result<Channel, Error> {
    let name = pipe_name(name);
    let handle = dacl::create_pipe(&name, true).map_err(|err| match err.kind() {
        // `FILE_FLAG_FIRST_PIPE_INSTANCE` answers ERROR_ACCESS_DENIED when the name is taken;
        // `AlreadyExists` is matched too so the refusal does not depend on one mapping.
        io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists => {
            Error::Transport(format!("another server is already listening on {name}"))
        }
        _ => Error::Transport(format!("could not create {name}: {err}")),
    })?;
    Ok(Channel { name, handle })
}

/// The full form of a pipe name: `[api.pipe] path` may be written bare (`tinio-mine`), and the
/// client classifies `state.control_name` by its `\\.\pipe\` prefix (Task 9), while
/// `CreateNamedPipeW` wants the full form — so the bind owns the normalisation and every reader
/// downstream sees one shape.
#[cfg(windows)]
fn pipe_name(name: &str) -> String {
    const PREFIX: &str = r"\\.\pipe\";
    if name.starts_with(PREFIX) {
        name.to_string()
    } else {
        format!("{PREFIX}{name}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use std::process;

    #[cfg(unix)]
    use tempfile::TempDir;

    use super::*;
    #[cfg(unix)]
    use crate::test_support::sock;

    #[cfg(unix)]
    #[test]
    fn a_second_bind_on_the_same_path_refuses() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let first = bind_unix(&path).expect("first bind");

        let err = bind_unix(&path).expect_err("second bind must refuse");
        assert!(
            format!("{err}").contains("already"),
            "names the reason: {err}"
        );
        drop(first);
    }

    #[cfg(unix)]
    #[test]
    fn a_stale_socket_file_is_reclaimed() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        // A socket file with no listener behind it is what a crashed instance leaves.
        let stale = UnixListener::bind(&path).expect("stale");
        drop(stale);

        let channel = bind_unix(&path).expect("a stale file must not block a restart");
        assert_eq!(channel.identity(), path.to_string_lossy());
        assert_eq!(channel.socket_path().as_deref(), Some(path.as_path()));
    }

    #[cfg(unix)]
    #[test]
    fn a_dry_run_reports_a_stale_socket_and_leaves_it() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        // The same residue `a_stale_socket_file_is_reclaimed` removes, only observed:
        // the dry-run arm is what `doctor` reports with before `--fix` calls it for real.
        let stale = UnixListener::bind(&path).expect("stale");
        drop(stale);

        assert!(
            reclaim_stale(&path, true).expect("probe"),
            "a stale file is what a fix reclaims"
        );
        assert!(path.exists(), "a dry run unlinks nothing");

        // A live listener is answered the same way a real call answers it.
        let _channel = bind_unix(&path).expect("bind");
        assert!(
            reclaim_stale(&path, true).is_err(),
            "a live socket is not residue"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_socket_file_is_private() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let _channel = bind_unix(&path).expect("bind");
        let mode = fs::metadata(&path).expect("metadata").permissions().mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "only the owner may talk to the control channel"
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_over_long_path_names_the_remedy() {
        let dir = TempDir::new().expect("temp dir");
        let err = bind_unix(&dir.path().join("a".repeat(120)))
            .expect_err("`sun_path` is 108 bytes at most");
        let text = format!("{err}");
        assert!(text.contains("too long"), "names the reason: {text}");
        assert!(text.contains("[api.unix] path"), "names the remedy: {text}");
    }

    #[cfg(windows)]
    #[test]
    fn a_second_bind_on_the_same_pipe_refuses() {
        // A bare name, normalised by the bind: what `state` records is the full `\\.\pipe\…`
        // form the client dials, so the normalisation is asserted here rather than assumed.
        let name = format!("tinio-test-{}", process::id());
        let first = bind_pipe(&name).expect("first bind");
        assert!(
            first.identity().starts_with(r"\\.\pipe\"),
            "{}",
            first.identity()
        );
        assert!(first.identity().ends_with(&name), "{}", first.identity());
        let err = bind_pipe(&name).expect_err("second bind must refuse");
        assert!(
            format!("{err}").contains("already"),
            "names the reason: {err}"
        );
        drop(first);
    }
}
