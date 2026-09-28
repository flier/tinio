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
use std::os::windows::io::{AsRawHandle as _, OwnedHandle, RawHandle};
#[cfg(unix)]
use std::{
    fs,
    os::{
        fd::{AsFd as _, AsRawFd as _, BorrowedFd},
        unix::{
            ffi::OsStrExt,
            fs::PermissionsExt,
            net::{UnixListener, UnixStream},
        },
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
/// instances the runtime adopts.
///
/// **The channel binds a pool, not one instance, and the pool is what keeps the name answering.**
/// A named pipe is dialled by name, so a client only ever finds an instance that is *listening*:
/// with a single instance the name goes deaf for as long as that instance is connected, and it
/// goes deaf for good inside an AppContainer, where the process that serves cannot create the next
/// one at all (see [`bind_pipe`]). `INSTANCES` instances are created here, the first with the
/// single-instance claim, and `transport::serve_pipe` runs one accept loop per instance, so while
/// any one of them is serving a connection another is still listening.
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
    /// The bound instances, created with their DACL and adopted inside the runtime
    /// (`dacl::adopt_pipe`, Task 4): `OwnedHandle`s rather than tokio's `NamedPipeServer`,
    /// because creating the latter needs the reactor the CLI does not have yet, and because
    /// they are `Send` where a raw pointer is not. The first carries the single-instance claim.
    pub(crate) handles: Vec<OwnedHandle>,
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

    /// The bound socket itself, borrowed — what a **launcher** hands the child it confines
    /// (sandbox design decision 16, extended to the control channel).
    ///
    /// **Why a launcher binds this at all.** The confined child's own `bind()` is a network
    /// operation the profile grants nothing for, so the launcher — which is unconfined — binds
    /// the channel and the child serves on the socket it inherits. A unix descriptor crosses
    /// `execve` for the same reason the data plane's listener does, and its `FD_CLOEXEC` has to
    /// be cleared for it to cross at all; **the clear is the launcher's half and cannot be the
    /// child's**, because with the flag set there is no descriptor left on the other side to act
    /// on. This borrow is that object: the fcntl takes it (`AsFd`), and
    /// [`BorrowedFd::as_raw_fd`](std::os::fd::AsRawFd::as_raw_fd) is the number
    /// `tinio_sandbox_core::control_fd_arg` spells into the child's argv — the element
    /// `Adopted::controls` reads back on the other side.
    ///
    /// **The single-instance claim stays the launcher's**, exactly as `bind_pipe`'s is: the
    /// bind that created this socket is what claimed the path, and the child adopts rather than
    /// binding, so nothing about "one server per root" is made weaker by the handoff.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # {
    /// use std::{os::fd::AsRawFd as _, path::Path};
    ///
    /// use tinio_api::bind::bind_unix;
    ///
    /// let channel = bind_unix(Path::new("/run/tinio/tinio-0123456789abcdef.sock")).expect("bind");
    /// // The object a launcher clears `FD_CLOEXEC` on, and the descriptor it spells for the
    /// // child.
    /// assert!(channel.socket().as_raw_fd() >= 0);
    /// # }
    /// ```
    pub fn socket(&self) -> BorrowedFd<'_> {
        self.listener.as_fd()
    }

    /// The descriptor a launcher duplicates into the child's plan.
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
    /// assert_eq!(channel.handoff_values().len(), 1);
    /// # }
    /// ```
    pub fn handoff_values(&self) -> Vec<i64> {
        vec![i64::from(self.socket().as_raw_fd())]
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

    /// Every bound instance as the raw handle a **launcher** hands to the child it confines
    /// (sandbox design decision 16, extended to the control channel).
    ///
    /// **Why a launcher needs them at all.** Inside a container the process's identity is
    /// the package SID and not the user, so a child that called [`bind_pipe`] itself would
    /// have `CreateNamedPipeW` refused — the DACL these instances are created with grants the
    /// current user and nobody else. The launcher runs unconfined, binds the pool here, and
    /// passes every handle; the child serves on the objects it inherited and never creates one.
    /// The DACL is therefore unchanged and the single-instance claim is still made exactly
    /// once, by the launcher's first create.
    ///
    /// **All of the pool and not just the first**, because a child holding one instance is back
    /// where it started: the name answers once and then goes deaf, which is the defect this
    /// handoff exists to remove. `INSTANCES` spells the count on both sides of the handoff.
    ///
    /// The handles are **borrowed**, never given up: the channel keeps ownership, so the
    /// caller must hold it for as long as the handoff needs and the child's own duplicated
    /// handles are what keep the pipe alive afterwards. `unsafe` appears nowhere here — a
    /// borrow of the handle is safe, and the one conversion back is
    /// [`dacl::adopted_pipe`]'s.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use tinio_api::bind::bind_pipe;
    ///
    /// let channel = bind_pipe("tinio-mine").expect("bind");
    /// // The values a `LaunchPlan::with_controls(..)` carries to the child.
    /// let handles = channel.raw_handles();
    /// assert!(handles.iter().all(|handle| !handle.is_null()));
    /// # }
    /// ```
    pub fn raw_handles(&self) -> Vec<RawHandle> {
        self.handles
            .iter()
            .map(|handle| handle.as_raw_handle())
            .collect()
    }

    /// Every pipe handle a launcher copies into the child's plan.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use tinio_api::bind::bind_pipe;
    ///
    /// let channel = bind_pipe("tinio-mine").expect("bind");
    /// assert!(!channel.handoff_values().is_empty());
    /// # }
    /// ```
    pub fn handoff_values(&self) -> Vec<i64> {
        self.raw_handles()
            .into_iter()
            .map(|raw| raw as usize as i64)
            .collect()
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

/// How many instances the bind creates.
///
/// **Two is the minimum that keeps the name answering, and the count is a contract rather than a
/// tuning knob.** One instance is connected for as long as it serves a client, and a client only
/// ever finds a *listening* instance, so one instance means the name is deaf while a request is in
/// flight — and, inside an AppContainer, deaf for good, because the serving process cannot create
/// its own successor (`create_pipe` is refused there; see [`bind_pipe`]). Two lets
/// `transport::serve_pipe` rotate: while one instance serves, the other is listening.
///
/// **The number travels with the handles, and the handoff end is checked.** [`bind_pipe`] creates
/// exactly this many, a launcher passes every one of them ([`Channel::raw_handles`] is what it
/// hands over), and `Plane::adopt` **refuses a handoff shorter than this constant** — so a launcher
/// that under-delivered fails the child's startup with a message about the deaf name rather than
/// serving a channel that answers once and then stops. Raising this number raises that floor with
/// it, which is why there is one constant here rather than a literal at each site.
#[cfg(windows)]
pub(crate) const INSTANCES: usize = 2;

/// Binds the named pipe `name` **as a pool of `INSTANCES` instances**, refusing when another
/// server already owns the name, and keeps the name on the channel (`NamedPipeServer` will not
/// hand it back).
///
/// **The first instance carries `FILE_FLAG_FIRST_PIPE_INSTANCE`, the rest do not**, and that is
/// the whole single-instance rule: the claim is made exactly once, by the process that binds, and
/// a second server's own first create is refused — while this channel's successors are ordinary
/// instances of a name it already owns. **Every instance comes from the same [`dacl::create_pipe`]
/// call**, so each carries the identical current-user-only DACL; nothing here widens a permission
/// or asks for one.
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
    let mut handles = Vec::with_capacity(INSTANCES);
    for nth in 0..INSTANCES {
        // The claim belongs to the first create and to nothing else. Re-asserting it on a
        // successor would refuse this channel's own pool, and dropping it from the first would
        // leave the single-instance gate open.
        let handle = dacl::create_pipe(&name, nth == 0).map_err(|err| match err.kind() {
            // `FILE_FLAG_FIRST_PIPE_INSTANCE` answers ERROR_ACCESS_DENIED when the name is taken;
            // `AlreadyExists` is matched too so the refusal does not depend on one mapping. The
            // successors carry no flag, so this answer is the first create's alone — but the
            // mapping is harmless on them and the message is the truthful one for a name someone
            // else owns.
            io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists if nth == 0 => {
                Error::Transport(format!("another server is already listening on {name}"))
            }
            _ => Error::Transport(format!("could not create {name}: {err}")),
        })?;
        handles.push(handle);
    }
    Ok(Channel { name, handles })
}

/// The full form of a pipe name: `[api.pipe] path` may be written bare (`tinio-mine`), and the
/// client classifies `state.control_name` by its `\\.\pipe\` prefix (Task 9), while
/// `CreateNamedPipeW` wants the full form — so the bind owns the normalisation and every reader
/// downstream sees one shape.
///
/// **Two callers, one normalisation**: [`bind_pipe`] creates the pool under this name,
/// and `Plane::adopt` records it as the inherited channel's identity, so the child's
/// `state.control_name` is the name its launcher actually bound rather than a second derivation
/// that could disagree. `pub(crate)` and not private for exactly that second caller, and it
/// carries no example of its own for the reason every `pub(crate)` item here does not: a doctest
/// is compiled against the public API alone. The rule it applies is asserted from the outside by
/// `a_second_bind_on_the_same_pipe_refuses`, which reads the normalised name off a real bind.
#[cfg(windows)]
pub(crate) fn pipe_name(name: &str) -> String {
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
