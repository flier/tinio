//! The inherited **control-channel** handle — one type, two `cfg` forms.
//!
//! **The forms differ because what a handoff needs differs.** On Unix the descriptor must be
//! duplicated so the launcher's own copy keeps every flag it had, and the platform crate has to
//! reach the duplicate as an *object* to clear its close-on-exec flag between the profile
//! pre-flight and the real spawn. On Windows nothing is reopened — the handle is marked
//! inheritable immediately before `CreateProcessW` — so only the value the launcher's channel
//! owns crosses.
//!
//! **The constructors do not share a signature, and that is deliberate**: Unix's `duplicate` can
//! fail (a full descriptor table, a closed socket) while Windows' `inherited` cannot, so each half
//! is written for its own platform rather than bent into one shape.

#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd as _, BorrowedFd, OwnedFd};

/// One inherited control-channel handle the launcher holds for the child it is about to spawn.
///
/// **Owned, on the platform whose descriptor needs a flag cleared in the launcher's process.**
/// A descriptor only survives the `execve` between the launcher and its child when its
/// close-on-exec flag is clear, and the platform crate is the only layer that knows *when* to
/// clear it: after the profile pre-flight and before the real spawn, so the throwaway probe
/// inherits the listener and nothing else. Reaching the descriptor at that moment is what needs
/// an object rather than a number — `rustix::io::fcntl_setfd` takes an `AsFd`, and nothing may
/// rebuild one from an integer (decision 14: that rebuild is one of the platform crate's three
/// enumerated `unsafe` sites, in the child).
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::ControlHandle;
    ///
    /// // The launcher has bound the channel; the plan takes a *duplicate*, so the socket it
    /// // bound keeps every flag it had.
    /// let socket = UnixListener::bind("/run/tinio/control.sock")?;
    /// let handle = ControlHandle::duplicate(&socket)?;
    /// assert!(handle.value() >= 0);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn duplicate(socket: impl AsFd) -> io::Result<Self> {
        socket.as_fd().try_clone_to_owned().map(Self)
    }

    /// The descriptor the child's argv will name, as the core spells inherited descriptors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, control_fd_arg};
    ///
    /// let socket = UnixListener::bind("/run/tinio/control.sock")?;
    /// let handle = ControlHandle::duplicate(&socket)?;
    /// // The number the child's argv carries, spelled by the one function that spells it.
    /// let element = control_fd_arg(handle.value());
    /// assert!(
    ///     element
    ///         .to_string_lossy()
    ///         .starts_with("tinio-sandbox-control-fd=")
    /// );
    /// # Ok::<(), std::io::Error>(())
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::ControlHandle;
    ///
    /// // The value a launcher's channel answered, copied for the plan — never a new owner.
    /// let handle = ControlHandle::inherited(1040);
    /// assert_eq!(handle.value(), 1040);
    /// ```
    pub fn inherited(value: i64) -> Self {
        Self(value)
    }

    /// The handle value the platform marks inheritable and names in the child's argv.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{ControlHandle, control_fd_arg};
    ///
    /// // The number the child's argv carries, spelled by the one function that spells it.
    /// let element = control_fd_arg(ControlHandle::inherited(1040).value());
    /// assert_eq!(element, "tinio-sandbox-control-fd=1040");
    /// ```
    pub fn value(&self) -> i64 {
        self.0
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        os::{
            fd::{AsFd as _, AsRawFd as _},
            unix::net::UnixStream,
        },
    };

    use super::ControlHandle;

    /// **The borrow is the whole reason the Unix form is owned** (decision 14:
    /// nothing in this crate may rebuild a descriptor from the integer that
    /// crossed argv), so what a test can pin is that the borrowed descriptor is
    /// the launcher's *duplicate* — the same socket, a different descriptor —
    /// rather than a second channel or the launcher's own end.
    ///
    /// **The peer proves it, not the number.** A descriptor compared by value
    /// would pass for any two fresh descriptors; what makes this the right one
    /// is that a write through it arrives on the socket the launcher bound,
    /// which is the socket `fcntl_setfd` would be clearing `FD_CLOEXEC` on.
    ///
    /// **The duplicate's own failure is not asserted here, and cannot be**: the
    /// only ways `try_clone_to_owned` fails are a closed descriptor and a full
    /// descriptor table, and this crate forbids the `unsafe` an already-closed
    /// `OwnedFd` would need to build. The refusal is the standard library's,
    /// passed through unchanged — there is nothing here that shapes it.
    #[test]
    fn the_handle_lends_its_own_duplicate_and_not_the_launchers_descriptor() {
        let (launcher, mut peer) = UnixStream::pair().expect("a socket pair");
        let handle = ControlHandle::duplicate(&launcher).expect("duplicate the channel");

        assert_eq!(
            handle.value(),
            i64::from(handle.as_fd().as_raw_fd()),
            "the argv number and the object the flag is cleared on are one descriptor"
        );
        assert_ne!(
            handle.value(),
            i64::from(launcher.as_raw_fd()),
            "and it is the duplicate, so the launcher's own copy keeps every flag it had"
        );

        let mut through_handle = UnixStream::from(
            handle
                .as_fd()
                .try_clone_to_owned()
                .expect("reopen the handle's descriptor"),
        );
        through_handle
            .write_all(b"control")
            .expect("the handle's own socket");
        let mut received = [0u8; 7];
        peer.read_exact(&mut received)
            .expect("the peer the launcher bound");
        assert_eq!(&received, b"control");
    }
}
