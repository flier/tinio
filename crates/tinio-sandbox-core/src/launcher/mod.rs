//! The launcher contract shared by the child-launching platforms.

pub mod payload;
pub mod relay;
pub mod supervise;

// The descriptor fixture the Unix plan leg opens: `cfg(unix)` because only the Unix
// `ControlHandle` is built from a real descriptor — Windows' is copied from a value the launcher's
// channel owns, so that leg spells a number instead — and `cfg(test)` because nothing else here
// reads a file.
#[cfg(all(test, unix))]
use std::fs;
#[cfg(any(unix, test))]
use std::io;
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd as _, BorrowedFd, OwnedFd};
use std::{ffi::OsString, io::Read, net::TcpListener};

use tracing::debug;

// `Adopted`, `adopt` and `LaunchPlan` are declared below in this module rather
// than in a submodule, so `lib.rs` re-exporting them as `launcher::{Adopted,
// adopt, LaunchPlan}` resolves. A `pub use self::adopt::…` would name a module
// that does not exist.
pub use self::{
    payload::{
        CONTROL_FD_PREFIX, LISTENER_FD_PREFIX, LaunchPayload, SandboxSpec, control_fd_arg,
        control_fd_from_arg, is_control_fd_arg, is_launcher_fd_arg, is_listener_fd_arg,
        listener_fd_arg, listener_fd_from_arg,
    },
    relay::{Observer, Terminal},
    supervise::{Child, supervise},
};
use crate::error::Result;

/// One inherited control-channel handle the launcher holds for the child it is about to spawn.
///
/// **Owned, on the platform whose descriptor needs a flag cleared in the launcher's process.**
/// A descriptor only survives the `execve` between the launcher and its child when its
/// close-on-exec flag is clear, and the platform crate is the only layer that knows *when* to
/// clear it: after the profile pre-flight and before the real spawn, so the throwaway probe
/// inherits the listener and nothing else. Reaching the descriptor at that moment is what needs
/// an object rather than a number — `rustix::io::fcntl_setfd` takes an `AsFd`, and nothing may
/// rebuild one from an integer (decision 14: that rebuild is the platform crate's single
/// enumerated `unsafe` site, in the child).
///
/// **The launcher's own copy is left alone.** The handle is a *duplicate*
/// ([`BorrowedFd::try_clone_to_owned`], which is `F_DUPFD_CLOEXEC`), so the socket the launcher
/// bound keeps every flag it had and the descriptor here is the handoff's alone. It closes when
/// the plan is dropped — after the child was spawned and holds its own copy.
///
/// **A descriptor is only reachable as a duplicate, and that is why [`Self::duplicate`] is the
/// only constructor.** `std` gives `OwnedFd` no safe route from a raw number: there is no
/// `From<i32>` and no `TryFrom<RawFd>`, and `OwnedFd::from_raw_fd` is an `unsafe fn` this crate
/// may not call (`unsafe_code = "forbid"`). Taking the *borrowed* descriptor and duplicating it
/// is the safe route, and it is also the truthful one for this handoff.
///
/// # Examples
///
/// ```no_run
/// use std::os::{fd::AsRawFd as _, unix::net::UnixListener};
///
/// use tinio_sandbox_core::ControlHandle;
///
/// let socket = UnixListener::bind("/run/tinio/control.sock")?;
/// // The duplicate the launcher hands the plan, and the number its argv element will spell.
/// let handle = ControlHandle::duplicate(&socket)?;
/// assert_ne!(
///     handle.value(),
///     i64::from(socket.as_raw_fd()),
///     "the plan owns a duplicate, not the launcher's own descriptor"
/// );
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(unix)]
#[derive(Debug)]
pub struct ControlHandle(OwnedFd);

/// One inherited control-channel handle the launcher holds for the child it is about to spawn.
///
/// **The Windows form of the type above, and it holds a value rather than owning one.** Nothing
/// on this platform needs the clear that motivates the Unix form: `SetHandleInformation(handle,
/// HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)` is the platform's own step, taken immediately
/// before `CreateProcessW`, so no caller has to reach an object between a pre-flight and a spawn.
/// And it could not own one here anyway — `OwnedHandle` has no safe constructor from a raw
/// handle (every `From` impl there takes an already-owned object), while the launcher holds
/// handle *values* borrowed from the `Channel` that owns them. Owning would force `unsafe` into
/// this crate, which is `forbid` throughout, so the value is carried as the number it is and the
/// owner stays the launcher's channel.
///
/// # Examples
///
/// ```no_run
/// use std::fs::File;
///
/// use tinio_sandbox_core::ControlHandle;
///
/// // A handle value the launcher's channel owns, copied for the plan — never a new owner.
/// let handle = ControlHandle::inherited(7);
/// assert_eq!(handle.value(), 7);
/// # let _ = File::open("Cargo.toml")?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlHandle(i64);

/// The one constructor both launchers build a plan through.
///
/// **Unix: the descriptor is duplicated, so ownership is real and the plan closes it.** The
/// duplicate is close-on-exec (`F_DUPFD_CLOEXEC`) and only the platform crate clears that, which
/// is the whole reason this type exists. The conversion is written in terms of the borrowed
/// descriptor rather than a number, and that is what keeps it safe: `try_clone_to_owned` needs a
/// `BorrowedFd`, so a caller cannot hand it an integer it made up.
///
/// **Windows: the value is copied, because the channel is the owner.** The handle is marked
/// inheritable just before `CreateProcessW` and nothing reopens it, so a duplicate would be a
/// second owner of a pipe the channel already owns — a handle closed twice, or closed while the
/// child still serves on it. **The two forms differ in signature because they differ in what they
/// do**: Unix's `duplicate` is fallible (a descriptor table that is full, or a closed socket) and
/// Windows' `inherited` cannot fail, so each is written for its own half rather than bent into one
/// shape — the callers are `#[cfg]`-selected platform code either way.
///
/// # Errors
///
/// The duplicate's failure on Unix — a descriptor table that is full, or a closed socket.
///
/// # Examples
///
/// ```no_run
/// use std::os::unix::net::UnixListener;
///
/// use tinio_sandbox_core::ControlHandle;
///
/// let socket = UnixListener::bind("/run/tinio/control.sock")?;
/// let handle = ControlHandle::duplicate(&socket)?;
/// assert!(handle.value() >= 0);
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(unix)]
impl ControlHandle {
    /// Duplicate `socket`, whose duplicate this value owns until the plan is dropped.
    ///
    /// # Errors
    ///
    /// The duplicate's failure.
    pub fn duplicate(socket: impl AsFd) -> io::Result<Self> {
        socket.as_fd().try_clone_to_owned().map(Self)
    }

    /// The descriptor the child's argv will name, as the core spells inherited descriptors.
    pub fn value(&self) -> i64 {
        i64::from(self.0.as_raw_fd())
    }
}

/// The object the clear is performed on — the whole reason the Unix form is owned.
///
/// `rustix::io::fcntl_setfd` takes the `AsFd` below, so the platform crate clears `FD_CLOEXEC`
/// on a real descriptor object and never reconstructs one from the integer that crossed argv.
#[cfg(unix)]
impl AsFd for ControlHandle {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

/// The Windows half of the one constructor — the handle **value** the launcher's channel owns.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::ControlHandle;
///
/// // The value a launcher's `Channel::raw_handles` answered, copied for the plan.
/// let handle = ControlHandle::inherited(7);
/// assert_eq!(handle.value(), 7);
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(windows)]
impl ControlHandle {
    /// The handle `value` the launcher's channel keeps owning, named for the child's argv.
    ///
    /// The value is one the launcher already holds, so nothing here can fail and the signature
    /// says so — the Unix form's `Result` is its own duplicate's, not a shape this one bends to.
    /// **Nothing is
    /// duplicated**, because this platform's handoff is a copied handle value: marking it
    /// inheritable is the whole step, and a duplicate would be a second owner of a pipe the
    /// child still serves on.
    pub fn inherited(value: i64) -> Self {
        Self(value)
    }

    /// The handle value the platform marks inheritable and names in the child's argv.
    pub fn value(&self) -> i64 {
        self.0
    }
}

/// Everything a launcher needs that is not an exposed path.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::LaunchPlan;
///
/// let plan = LaunchPlan::default();
/// assert!(plan.payload().is_none());
/// assert!(plan.listener().is_none());
/// assert!(plan.controls().is_empty());
/// assert!(plan.token().is_none());
/// ```
#[derive(Default, derive_more::Debug)]
pub struct LaunchPlan {
    payload: Option<LaunchPayload>,
    listener: Option<TcpListener>,
    /// The inherited control-channel handles, in the order the launcher bound them. A `Vec`
    /// rather than one value because the channel is a **pool**: one instance answers a client and
    /// then stops listening, so the child needs every instance the launcher created (see
    /// [`Self::with_controls`]).
    controls: Vec<ControlHandle>,
    /// The management plane's token, when the launcher generated one (see
    /// [`Self::with_token`]).
    ///
    /// `#[debug(skip)]` because it is a bearer credential: a plan is the kind of value that
    /// reaches a log line through a `?` on a failing launch, and the token guards `/status` and
    /// `/stop`.
    #[debug(skip)]
    token: Option<String>,
}

impl LaunchPlan {
    /// An empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carry a payload.
    pub fn with_payload(mut self, payload: LaunchPayload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// The payload, when one was set.
    pub fn payload(&self) -> Option<&LaunchPayload> {
        self.payload.as_ref()
    }

    /// Carry the management plane's **token** — the value the child verifies `/status` and
    /// `/stop` against, and the value the launcher publishes in `state` (design decision 17's
    /// private channel, extended to the one secret the two processes share).
    ///
    /// **The token lives on the plan and not in the payload it was set with**, because the
    /// platform is what serializes the payload: `spawn_supervised_child` builds the frame from
    /// `config()` and this value at the moment it writes the child's stdin, so the plan is the
    /// one place the token is set, and the payload is a spelling of it on the wire rather than a
    /// second source. It is private launcher→child data for the reason the payload is stdio: an
    /// argv element or an environment variable is readable by other processes, and this is the
    /// only credential either process holds.
    ///
    /// **A plan with no token is the ordinary plan.** Nothing generates one here: the launcher
    /// that will publish `state` is the only caller that has a reason to, and a plan built for
    /// any other launch carries none.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// let token = "00".repeat(32);
    /// let plan = LaunchPlan::for_launch("port = 9000".into()).with_token(token.clone());
    /// assert_eq!(plan.token(), Some(token.as_str()));
    /// ```
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    /// The token, when the launcher set one.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// **Own** the bound listener the child will serve on (design decision 16).
    ///
    /// Owned rather than named by descriptor: the parent must keep the socket
    /// alive for the child's whole lifetime, and a plan that carried a bare
    /// number while the socket lived somewhere else would let the two drift
    /// apart with nothing to notice it. The platform derives the descriptor
    /// when it builds the child's argv.
    ///
    /// The listener lives **here and not in the payload** (design §1: the plan
    /// holds what the launcher needs that is not a path) — carrying it in both
    /// would be two sources of truth for one fact.
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The owned listener, when the parent bound one.
    pub fn listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// Carry the inherited **control channel's** handles (design decision 16, extended to
    /// the control plane).
    ///
    /// Each handle is a [`ControlHandle`] rather than a bare number, and that type's own doc
    /// carries why: the platform crate has to reach the descriptor **after** the profile
    /// pre-flight and clear its close-on-exec flag there, so that the throwaway probe inherits
    /// **neither** descriptor — the clear is what comes *after* the probe, and one performed
    /// before it would hand the probe the launcher's listening sockets. The listener takes the
    /// same treatment from the other side: it is owned by the plan as a socket, and its clear
    /// happens at the same moment.
    ///
    /// The handles travel as the raw values the platform turns back into the channel's objects,
    /// exactly as [`Adopted::controls`] spells them, and for the listener's reason in reverse:
    /// the listener socket is *owned* here because the parent must keep it alive, while the
    /// control channel is owned by the parent only long enough to hand it over — the child serves
    /// on the objects it inherited.
    ///
    /// **A set, because one platform's channel is a pool and the other's is not.** On Windows the
    /// channel is a pool of pipe instances, and a client only ever reaches one that is
    /// *listening*: the launcher creates them all (only it can — the DACL is current-user-only and
    /// the child's identity is the package SID), and the child serves on all of them so that one
    /// instance serving a request does not leave the name deaf — handing over fewer than the
    /// launcher bound is a channel that answers once and then stops. On macOS the channel is a
    /// single unix socket, and the reason it travels at all is the profile rather than an
    /// identity: the confined child's `bind()` is a network operation that profile grants nothing
    /// for, so the launcher binds the socket outside the sandbox and the child adopts the
    /// descriptor. One entry there, and the platform refuses any other count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// // Two bound sockets to hand over — the fixture, not the subject: what this leg asserts is
    /// // the plan's own ordering and count, and each value is whatever the platform's duplicate
    /// // answered.
    /// let left = ControlHandle::duplicate(UnixListener::bind("/run/tinio/left.sock")?)?;
    /// let right = ControlHandle::duplicate(UnixListener::bind("/run/tinio/right.sock")?)?;
    /// let plan = LaunchPlan::default().with_controls([left, right]);
    /// assert_eq!(plan.controls().len(), 2);
    ///
    /// // The single-handle spelling is the same fact: a plan that carries one instance.
    /// assert_eq!(
    ///     LaunchPlan::default()
    ///         .with_control(ControlHandle::duplicate(UnixListener::bind(
    ///             "/run/tinio/one.sock"
    ///         )?)?)
    ///         .controls()
    ///         .len(),
    ///     1
    /// );
    /// # Ok(())
    /// # }
    /// # #[cfg(not(unix))]
    /// # fn main() {}
    /// ```
    pub fn with_controls(mut self, handles: impl IntoIterator<Item = ControlHandle>) -> Self {
        self.controls = handles.into_iter().collect();
        self
    }

    /// Carry **one** inherited control-channel handle — [`Self::with_controls`] with a single
    /// element, for the callers whose channel is one instance: macOS's unix socket, and a
    /// one-instance pipe.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// let handle = ControlHandle::duplicate(UnixListener::bind("/run/tinio/control.sock")?)?;
    /// let plan = LaunchPlan::default().with_control(handle);
    /// assert_eq!(plan.controls().len(), 1);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(unix))]
    /// # fn main() {}
    /// ```
    pub fn with_control(self, handle: ControlHandle) -> Self {
        self.with_controls([handle])
    }

    /// The inherited control channel's handles, in the order the launcher bound them.
    pub fn controls(&self) -> &[ControlHandle] {
        &self.controls
    }

    /// The configuration text the launcher forwards **unparsed** (design
    /// decision 18). Empty when no payload was set.
    pub fn config(&self) -> &str {
        self.payload
            .as_ref()
            .map_or("", |payload| payload.config.as_str())
    }

    /// Build the plan a parent uses before launching a child.
    ///
    /// No listener parameter: [`Self::with_listener`] carries it, so a caller
    /// cannot set the same fact in two places.
    ///
    /// Infallible because the payload it builds is one string. The earlier shape
    /// had to generate a nonce here and was `Result` for it; with the nonce gone
    /// (design decision 17) there is nothing left to fail.
    pub fn for_launch(config: String) -> Self {
        Self::default().with_payload(LaunchPayload {
            config,
            token: None,
            confinement: None,
        })
    }
}

/// What a confined child recovers from its own launch (design decision 17).
///
/// **Construction goes through [`Adopted::new`] or [`adopt`], and reads go through the
/// accessors.** `config`, `control_fds` and `token` are crate-private on purpose: a `pub` field
/// beside an accessor that returns the same value is one fact with two public spellings, and the
/// pair drifts the moment either grows a rule of its own. `listener_fd` is the exception and stays
/// public, because the Windows platform's `launch::listener` reads the field directly.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Adopted;
///
/// let adopted = Adopted::new("port = 9000", Some(7), vec![9], None, None);
/// assert_eq!(adopted.listener_fd, Some(7));
/// assert_eq!(adopted.controls(), [9]);
/// assert_eq!(adopted.token(), None);
/// assert!(adopted.confinement().is_none());
/// ```
#[derive(Clone, PartialEq, Eq, derive_more::Debug)]
pub struct Adopted {
    /// The configuration text the parent forwarded, unparsed.
    ///
    /// `#[debug(skip)]` for the same reason [`LaunchPayload::config`] carries
    /// it: this text can hold credentials, and a `Debug` that prints them leaks
    /// them into the first log line, `dbg!` or `unwrap_err()` that happens to
    /// touch a value containing this one. A caller that needs to see the
    /// configuration prints it deliberately.
    #[debug(skip)]
    pub(crate) config: String,
    /// The inherited listener's descriptor, which only the platform can turn
    /// back into a socket.
    pub listener_fd: Option<i64>,
    /// The inherited control channel's handles, when the parent created the channel — a
    /// launcher platform, either because a container's process cannot create the pipes its
    /// current-user-only DACL needs (Windows) or because the confinement's profile grants no
    /// `bind()` for a socket (macOS).
    ///
    /// Spelled exactly like [`Adopted::listener_fd`], and for the same reason: only
    /// the platform can turn the values back into the objects they name, so the core
    /// carries numbers and the conversion stays in the platform crate.
    ///
    /// **One entry per instance the launcher bound**, in that order: the channel is a pool, and
    /// the child serves on every instance it was handed so that one of them serving a request does
    /// not leave the name deaf.
    pub(crate) control_fds: Vec<i64>,
    /// The management plane's token, when the launcher sent one: the value the
    /// child's plane verifies `/status` and `/stop` against, and the value the
    /// launcher already published in `state`.
    ///
    /// **The child adopts it rather than generating one**, which is the whole
    /// point of the field: the token a client reads out of `state` is the
    /// launcher's, so a plane that minted its own would refuse every client that
    /// found it — `/status` and `/stop` would answer `401` for a server that is
    /// running and discoverable. `None` is every launch that carried none: the
    /// unconfined runs, the in-place platforms, and the confined shapes where the
    /// launcher has no `state` file to own (it then generates its own token and
    /// publishes as it always did).
    ///
    /// `#[debug(skip)]`: a bearer credential earns the same treatment in `Debug`
    /// as the configuration text above.
    #[debug(skip)]
    pub(crate) token: Option<String>,
    /// What this process must do to confine **itself**, when its launcher's
    /// mechanism hands it that job rather than wrapping its `exec`.
    ///
    /// **The child never reads the mechanism's environment variable.** The
    /// launcher resolves the mechanism, renders the profile and states both here,
    /// so the confinement a child applies is the one its launcher chose even if
    /// the environment changed in between — and `None`, the ordinary case, is
    /// every launch whose confinement was already applied to it (`sandbox-exec`
    /// and Windows) or that was never confined at all.
    ///
    /// **Printed by `Debug`**, unlike the two fields above: the profile names no
    /// path and holds no secret — the same bytes `sandbox-exec` carries in its
    /// own process list — and a refusal here is diagnosed from exactly this text.
    pub(crate) confinement: Option<SandboxSpec>,
}

/// Recover a launch, or decide this process is the launcher.
///
/// **Reads `stdin` only when `args` carries the listener element.** A process
/// started from a shell owns its stdin — a terminal, a heredoc, another tool's
/// pipe — so a blind read would block a launcher that nothing launched, waiting
/// for input that is never coming, and present as a hang rather than a refusal.
///
/// Returns `Ok(None)` for "this is the launcher": no element, or an element
/// whose payload does not decode. Both are ordinary outcomes, and both fail
/// closed if the process turns out to be inside a confinement after all.
///
/// Arguments are taken rather than read from the environment so the whole rule
/// is unit-testable without a subprocess.
///
/// Which of those outcomes the call reached is traced (`step = "adopt"`): that
/// an element was present and whether a payload decoded, never the payload's
/// contents, which carry credentials.
///
/// # Examples
///
/// ```
/// use std::{ffi::OsString, io};
///
/// use tinio_sandbox_core::adopt;
///
/// // No element: the launcher, and stdin is never touched.
/// let none = adopt(Vec::<OsString>::new(), io::empty()).unwrap();
/// assert!(none.is_none());
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    mut stdin: impl Read,
) -> Result<Option<Adopted>> {
    // Collected once, because **two** elements are read out of the same list: the
    // listener's decides whether this process is a child at all, and the control
    // channel's is optional. A second pass over the iterator is not available, and a
    // second `adopt` call would read stdin twice.
    let args: Vec<OsString> = args.into_iter().collect();
    let Some(listener_fd) = args.iter().find_map(|arg| listener_fd_from_arg(arg)) else {
        debug!(step = "adopt", listener = false, payload = "absent");
        return Ok(None);
    };
    // Only read when a listener element is present, so a launcher nothing launched
    // still never touches its own stdin. A control element alone is not a launch:
    // the listener element is what a launcher always writes (design §10 step 6).
    // The control elements are a **set**: the launcher writes one per instance, so this collects
    // every one rather than the first. The listener element above is still the only thing that
    // decides this process is a child — a control element on its own is a forged argv, and the
    // fail-safe that rule protects is `a_control_element_alone_is_not_a_launch`.
    let control_fds: Vec<i64> = args
        .iter()
        .filter_map(|arg| control_fd_from_arg(arg))
        .collect();
    let mut text = String::new();
    // The parent writes the payload and closes; a read to EOF is the whole
    // protocol, and a failure to read is a payload failure rather than an IO
    // error the caller could act on differently.
    if stdin.read_to_string(&mut text).is_err() {
        debug!(step = "adopt", listener = true, payload = "unreadable");
        return Ok(None);
    }
    // `serde_json::from_str` already rejects trailing bytes after the object,
    // which is the property that matters: a child must be certain the whole
    // stream was meant for it.
    match serde_json::from_str::<LaunchPayload>(&text) {
        Ok(payload) => {
            debug!(step = "adopt", listener = true, payload = "decoded");
            Ok(Some(Adopted::new(
                payload.config,
                Some(listener_fd),
                control_fds,
                payload.token,
                payload.confinement,
            )))
        }
        Err(_) => {
            debug!(step = "adopt", listener = true, payload = "rejected");
            Ok(None)
        }
    }
}

impl Adopted {
    /// Build the value a child holds — the configuration text, the inherited descriptors and
    /// the launcher's token — each as [`adopt`] would have read it.
    ///
    /// **The one public constructor.** [`adopt`] is the other way to one of these — the one a real
    /// child uses, decoding its launch payload — and it builds through this function, so a caller
    /// assembling the shape by hand (a test driving a role) produces exactly what the wire
    /// produces rather than a second spelling of it. Reads go through [`Self::config`],
    /// [`Self::listener`], [`Self::controls`] and [`Self::token`]: the fields are private on
    /// purpose (the struct's own doc says why), `listener_fd` excepted.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new("port = 9000", Some(7), Vec::new(), None, None);
    /// assert_eq!(adopted.config(), "port = 9000");
    /// assert_eq!(adopted.listener(), Some(7));
    /// assert!(adopted.controls().is_empty());
    /// ```
    pub fn new(
        config: impl Into<String>,
        listener_fd: Option<i64>,
        control_fds: Vec<i64>,
        token: Option<String>,
        confinement: Option<SandboxSpec>,
    ) -> Self {
        Self {
            config: config.into(),
            listener_fd,
            control_fds,
            token,
            confinement,
        }
    }

    /// The configuration text the parent forwarded, unparsed (design decision
    /// 18).
    ///
    /// Empty when the parent sent none, which is the ordinary case for a launch
    /// that took its configuration from somewhere else.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new("port = 9000", None, Vec::new(), None, None);
    /// assert_eq!(adopted.config(), "port = 9000");
    /// ```
    pub fn config(&self) -> &str {
        &self.config
    }

    /// The inherited listener's descriptor, when the parent handed one over.
    ///
    /// The descriptor is still the platform's to turn into a socket: only
    /// `from_raw_socket` or an `OwnedFd` conversion knows what it names, which
    /// is why the conversion stays in the platform crate.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
    /// assert_eq!(adopted.listener(), Some(7));
    /// ```
    pub fn listener(&self) -> Option<i64> {
        self.listener_fd
    }

    /// The inherited control channel's handles, when the parent created the channel.
    ///
    /// **What the child does with them is the whole point**: a non-empty answer means the
    /// launcher already created the control channel — its pipes exist with a DACL only the
    /// unrestricted launcher could attach, or its socket is bound where only the unconfined
    /// process may bind one — so the child adopts those objects rather than creating a second
    /// one. Empty is every other shape of launch: an in-place platform, an `api`-less build, and
    /// a run whose selection carries no local channel at all.
    ///
    /// One entry per instance, and the child serves on all of them: a Windows channel needs more
    /// than one so that a client can reach a listening instance while another serves, while a
    /// macOS channel is one socket and the platform refuses any other count. The single-handle
    /// spelling this replaced answered `Option<i64>`, which is the shape a pool cannot use.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), vec![9, 13], None, None);
    /// assert_eq!(adopted.controls(), [9, 13]);
    /// ```
    pub fn controls(&self) -> &[i64] {
        &self.control_fds
    }

    /// The management plane's token, when the launcher sent one.
    ///
    /// **`Some` is the launcher's token and not the child's to replace.** The value a client
    /// reads out of `state` is this one, so the plane the child builds is the plane that token
    /// guards — the `token` field's own doc carries the refusal a child that minted its own
    /// would produce. `None` is every launch whose management plane publishes its own `state`
    /// and therefore generates its own token.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), vec![9], Some("00".repeat(32)), None);
    /// assert_eq!(adopted.token().map(str::len), Some(64));
    /// ```
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// What this process must do to confine **itself**, when its launcher's
    /// mechanism handed it that job.
    ///
    /// **`Some` is a command, not a hint**: the child is the only process that can
    /// apply a profile to itself, and the process that applied it is one-shot and
    /// irreversible, so a child that met one of these and served anyway would be
    /// serving unconfined under a launcher that reported a confinement. `None` is
    /// every launch whose confinement was already applied — `sandbox-exec`
    /// wrapping the `exec`, or Windows' container at process creation — and every
    /// unconfined run.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
    /// assert!(adopted.confinement().is_none());
    /// ```
    pub fn confinement(&self) -> Option<&SandboxSpec> {
        self.confinement.as_ref()
    }
}

#[cfg(test)]
mod tests {
    // `io` and `fs` are not imported here: the module above does — `io` for the constructor's
    // return type, `fs` for the descriptor fixture below — and `use super::*` brings both bindings
    // in, so a second import of either is a duplicate.
    use std::{cell::RefCell, ffi::OsString, fmt::Debug, mem, sync::Once};

    use tracing::{
        Event, Subscriber,
        field::{Field, Visit},
        subscriber::set_global_default,
    };
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt as _},
        registry,
    };

    use super::*;

    /// The fields of one recorded event that the assertion below names.
    ///
    /// Captured as a field rather than as rendered text, which is the point: a
    /// test that matched on a formatted message would pass for a log line that
    /// merely mentioned the words.
    #[derive(Debug, Default)]
    struct Recorded {
        step: Option<String>,
        payload: Option<String>,
    }

    impl Visit for Recorded {
        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "step" => self.step = Some(value.to_owned()),
                "payload" => self.payload = Some(value.to_owned()),
                _ => {}
            }
        }

        fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
    }

    thread_local! {
        /// What [`Recorder`] captured **on this thread**.
        ///
        /// Per-thread, while the recorder itself is process-wide, and the two
        /// halves answer two different problems: the subscriber must be global for
        /// the callsites' cached interest to be computed against it (see
        /// [`install_recorder`]), and the buffer must be per-thread for an
        /// exact-equality assertion to read the events of *its* test rather than
        /// every event the binary emitted through the same callsites.
        static EVENTS: RefCell<Vec<Recorded>> = const { RefCell::new(Vec::new()) };
    }

    /// A subscriber that records what the traced operations emitted, so "it
    /// logs" is a property a test can lose.
    #[derive(Debug)]
    struct Recorder;

    impl<S: Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut recorded = Recorded::default();
            event.record(&mut recorded);
            if recorded.step.is_some() {
                EVENTS.with_borrow_mut(|events| events.push(recorded));
            }
        }
    }

    /// Install [`Recorder`] as the **process-wide** subscriber, once.
    ///
    /// **Global rather than scoped, and that is the fix rather than a preference.**
    /// `tracing` caches every callsite's *interest* in a process-wide registry, and
    /// that interest is recomputed from the dispatchers installed when the cache is
    /// rebuilt: with no subscriber installed anywhere, a rebuild caches `never`
    /// for the callsites `adopt` emits through, and an event emitted in that window
    /// is dropped instead of recorded. `with_default` installs a *thread-local*
    /// dispatcher, and it does not take part in another thread's rebuild — so a
    /// concurrently running test was enough to lose one of the three events this
    /// module counts, measured at 3 runs in 40. A global default takes part in
    /// every rebuild, so those callsites stay `sometimes` from here on, and
    /// `set_global_default` rebuilds once itself, which clears whatever was cached
    /// before it.
    ///
    /// The error is ignored rather than unwrapped: another owner of the process
    /// default is not this test's failure, and no test in this crate sets one
    /// today.
    fn install_recorder() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = set_global_default(registry().with(Recorder));
        });
    }

    /// Take everything [`Recorder`] captured on this thread.
    fn recorded() -> Vec<Recorded> {
        EVENTS.with_borrow_mut(mem::take)
    }

    /// A reader that fails the test if anything reads from it.
    ///
    /// An EOF-returning reader cannot tell the two behaviours apart: `io::empty()`
    /// and a `Cursor` over an empty buffer behave identically whether or not they
    /// are read, so a blind read-to-end would pass a test built on one. This is
    /// hostile to being read at all, which is the property under test.
    struct Untouchable;

    impl io::Read for Untouchable {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("`adopt` read stdin without a listener element in argv");
        }
    }

    #[test]
    fn adopt_does_not_touch_stdin_without_the_listener_element() {
        // The fail-safe against a launcher blocking forever on an operator's
        // terminal: a process nothing launched owns its stdin, and reading it
        // would hang rather than refuse.
        assert!(
            adopt(Vec::<OsString>::new(), Untouchable)
                .unwrap()
                .is_none()
        );
        assert!(
            adopt([OsString::from("--port")], Untouchable)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn adopt_returns_the_configuration_text() {
        // The payload travels through `adopt` here rather than only through the
        // doc example, which never reaches it.
        let adopted = adopt(
            [listener_fd_arg(7)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#.to_vec()),
        )
        .unwrap()
        .expect("a listener element and a decodable payload");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(
            adopted.controls(),
            [] as [i64; 0],
            "a launch that named no control channel hands none back"
        );
        assert_eq!(
            adopted.token(),
            None,
            "a frame with no token field carries no token"
        );
    }

    #[test]
    fn adopt_hands_back_the_token_the_frame_carried() {
        // The third thing a payload can carry, and the one the child may not invent: the plane
        // this process builds verifies against the launcher's token, so an `adopt` that dropped
        // it would leave every `/status` and `/stop` on a discoverable server answering 401.
        let token = "00".repeat(32);
        let frame = format!(r#"{{"config":"port = 9000","token":"{token}"}}"#);
        let adopted = adopt([listener_fd_arg(7)], io::Cursor::new(frame))
            .unwrap()
            .expect("an element and a decodable payload");
        assert_eq!(adopted.token(), Some(token.as_str()));
    }

    #[test]
    fn adopt_hands_back_the_confinement_the_frame_carried() {
        // The fourth thing a payload can carry, and the one a child acts on rather than reads:
        // a mechanism that confines the child in-process puts the render and its values here,
        // and an `adopt` that dropped them would leave that child serving **unconfined** under a
        // launcher that reported a confinement — the failure decision 5's fail-closed rule is
        // about. The absent direction is the other half: every launch whose confinement was
        // already applied carries none, and `None` is what keeps `sandbox-exec`'s child and
        // Windows' reading the frame they were written against.
        let frame = r#"{"config":"port = 9000","confinement":{"mechanism":"sandbox-init","profile":"(deny default)","params":[["E0","/srv/data"]]}}"#;
        let adopted = adopt([listener_fd_arg(7)], io::Cursor::new(frame))
            .unwrap()
            .expect("an element and a decodable payload");
        let confinement = adopted.confinement().expect("the frame carried one");
        assert_eq!(confinement.mechanism, "sandbox-init");
        assert_eq!(confinement.profile, "(deny default)");
        assert_eq!(confinement.param("E0"), Some("/srv/data"));

        assert!(
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new(br#"{"config":"port = 9000"}"#)
            )
            .unwrap()
            .expect("the same frame without the field")
            .confinement()
            .is_none()
        );
    }

    #[test]
    fn adopt_reads_both_inherited_descriptors_out_of_one_argv() {
        // The two elements are read in the same pass, and neither stands in for the
        // other: a child of the Windows launcher inherits a listener and a control
        // pipe, so an `adopt` that answered one of them would leave the other's
        // handoff silently unconsumed.
        let adopted = adopt(
            [listener_fd_arg(7), control_fd_arg(9)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#),
        )
        .unwrap()
        .expect("both elements and a decodable payload");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(adopted.controls(), [9]);
    }

    #[test]
    fn adopt_reads_every_control_element_the_launcher_wrote() {
        // The channel is a pool, so the launcher writes one element per instance and `adopt` must
        // hand back *all* of them: a child given one instance answers a client and then stops
        // listening, which is the defect the pool exists to close.
        let adopted = adopt(
            [listener_fd_arg(7), control_fd_arg(9), control_fd_arg(13)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#),
        )
        .unwrap()
        .expect("three elements and a decodable payload");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(
            adopted.controls(),
            [9, 13],
            "every instance the launcher bound, in its order"
        );
    }

    #[test]
    fn a_control_element_alone_is_not_a_launch() {
        // The listener element is what a launcher always writes, and it is what decides
        // whether this process is a child at all. A control element on its own is a
        // forged argv, and reading stdin for it would hang an operator's terminal —
        // the fail-safe `adopt_does_not_touch_stdin_without_the_listener_element`
        // states, restated for the element beside it.
        assert!(
            adopt([control_fd_arg(9)], Untouchable).unwrap().is_none(),
            "a control element without a listener element is not a launch"
        );
    }

    /// Handles for the plan legs below, in the shape each platform's `ControlHandle` takes.
    ///
    /// **A real descriptor on Unix and a bare value on Windows**, which is the same difference the
    /// type itself carries: Unix's constructor duplicates a descriptor (so the test opens one) and
    /// Windows' copies the handle the launcher's channel owns (so the test spells one). The
    /// ordering assertion below needs two distinct values, and each platform's constructor is what
    /// makes them distinct.
    fn control_handles() -> (ControlHandle, ControlHandle) {
        #[cfg(unix)]
        {
            let duplicate = || {
                let source = fs::File::open("/dev/null").expect("a descriptor to duplicate");
                ControlHandle::duplicate(&source).expect("duplicate a descriptor")
            };
            (duplicate(), duplicate())
        }
        #[cfg(windows)]
        {
            let copy = |value| ControlHandle::inherited(value);
            (copy(1040), copy(1044))
        }
    }

    #[test]
    fn the_plan_carries_the_inherited_control_handles() {
        // The accessor pair, and the `Default` both `bind`ers rely on: a plan that
        // never named a control handle answers empty, which is the shape of every
        // `api`-less launch and of a launcher run whose selection names no local
        // channel. It is not the *unix* shape: macOS is a unix launcher platform and
        // carries one when it has a local channel.
        let (first, second) = control_handles();
        assert_ne!(
            first.value(),
            second.value(),
            "each duplicate is its own descriptor, so the plan's ordering is readable"
        );
        assert!(LaunchPlan::default().controls().is_empty());
        assert_eq!(
            LaunchPlan::new().with_control(first).controls().len(),
            1,
            "the single-handle spelling is a one-instance channel"
        );
        let (third, fourth) = control_handles();
        let plan = LaunchPlan::new()
            .with_controls([third, fourth])
            .controls()
            .iter()
            .map(ControlHandle::value)
            .collect::<Vec<_>>();
        assert_eq!(plan.len(), 2, "a pool is every instance the launcher bound");
        assert_ne!(
            plan[0], plan[1],
            "the pool is a set of handles, in its order: {plan:?}"
        );
        // And it rides beside the other two facts rather than replacing one: the
        // listener and the control handles are separate handoffs.
        let plan = LaunchPlan::for_launch("port = 9000".into())
            .with_controls([second])
            .with_payload(LaunchPayload {
                config: "port = 9001".into(),
                token: None,
                confinement: None,
            });
        assert_eq!(plan.controls().len(), 1);
        assert_eq!(plan.config(), "port = 9001");
    }

    #[test]
    fn the_plan_carries_the_token_beside_the_rest() {
        // The fourth fact a plan can carry, and the two halves that make it one: a plan that
        // never called `with_token` answers `None` — every unconfined and in-place launch — and
        // one that did answers the token the platform will put in the payload.
        assert_eq!(LaunchPlan::default().token(), None);
        let token = "ab".repeat(32);
        let plan = LaunchPlan::for_launch("port = 9000".into()).with_token(token.clone());
        assert_eq!(plan.token(), Some(token.as_str()));
        assert_eq!(
            plan.config(),
            "port = 9000",
            "the token rides beside the payload, replacing nothing"
        );
    }

    #[test]
    fn adopt_logs_its_outcome_and_why() {
        // `adopt` is the operation of the traced four that needs no child
        // process: whether a listener element was present and whether a payload
        // decoded is the whole decision it makes.
        install_recorder();
        // Whatever this thread recorded before now is not this test's business:
        // the harness may run more than one test on one thread.
        let _ = recorded();
        let (launcher, adopted, rejected) = (
            adopt(Vec::<OsString>::new(), io::empty()),
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new(r#"{"config":"port = 9000"}"#),
            ),
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new("not a payload at all"),
            ),
        );

        // The outcomes the events describe, so the test is about the events and
        // not about the returns.
        assert!(
            launcher.unwrap().is_none(),
            "no element: this process is the launcher"
        );
        assert_eq!(
            adopted
                .unwrap()
                .expect("an element and a decodable payload")
                .listener(),
            Some(7)
        );
        assert!(
            rejected.unwrap().is_none(),
            "an element whose payload does not decode"
        );

        // One event per call, each naming its operation — and each saying why,
        // which is the half a diagnosis needs.
        let events = recorded();
        let steps: Vec<_> = events.iter().map(|event| event.step.as_deref()).collect();
        assert_eq!(
            steps,
            [Some("adopt"), Some("adopt"), Some("adopt")],
            "{events:?}"
        );
        let payloads: Vec<_> = events
            .iter()
            .map(|event| event.payload.as_deref())
            .collect();
        assert_eq!(
            payloads,
            [Some("absent"), Some("decoded"), Some("rejected")],
            "{events:?}"
        );
    }
}
