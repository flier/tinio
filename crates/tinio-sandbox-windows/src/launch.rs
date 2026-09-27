//! The launcher's own plumbing: the child's adoption of the payload it was
//! started with, the inherited listener it rebuilds from that, and the argv
//! elements that name the inherited handles in the first place (design §3,
//! decisions 16 and 17).
//!
//! **The frame is the core's; the socket is this crate's.** [`adopt`] wraps the
//! core's rule — the whole decision (no element, an element with an undecodable
//! payload, an element with a good one) lives in `tinio-sandbox-core` and is
//! unit-tested there, because it takes the arguments and the reader as
//! parameters — and what this module adds to it is the two inputs that are *this*
//! process's: its argv and its stdin. The listener is the other way round:
//! turning a handle value back into a `TcpListener` is `unsafe`, so it belongs in
//! exactly one module ([`crate::socket`], decision 14), and all this module does
//! is unwrap the descriptor the core handed back. The **control channel's** handles
//! take the same route out of the core and a different one out of this crate: the
//! named pipes are adopted by `tinio-api` ([`adopt`]'s `Adopted::controls` names
//! them), because the crate that serves the channel is the crate that owns the raw
//! conversion. There is one per instance, and [`command_args`] writes them all.
//!
//! **What is deliberately not here.** The payload *write* is
//! [`crate::handles::write_payload`] with `serde_json` at the call site, so no
//! wrapper re-spells those two. The stop hook is the other way round: the
//! mechanism, the Win32 calls and their safety arguments are
//! [`crate::control`]'s (design decision 14), and [`on_stop`] and [`disarm`] are
//! the crate-level names the child side and a process that outlives one launch
//! reach for — the shape the shared surface fixes, so that `serve`'s child path
//! names one function on every platform.

use std::{
    ffi::OsString,
    io::{self, Read},
    net::TcpListener,
};

use windows_sys::Win32::Foundation::HANDLE;

// The core's `adopt` is aliased because this module's own wrapper owns that
// name. The core's `launch` needs no alias: no item here is called that, and a
// module's own name is not in its own scope.
use crate::_core::{Adopted, Result, adopt as core_adopt, launch};
/// Re-export the core's element helpers so the child side of the call site reads
/// symmetrically, consuming the core's names rather than aliasing them.
pub use crate::_core::{
    CONTROL_FD_PREFIX, LISTENER_FD_PREFIX, control_fd_arg, control_fd_from_arg, is_control_fd_arg,
    is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg,
};
use crate::{control, socket};

/// Adopt the launch payload, if this process inherited one.
///
/// Core's rule, called on this process's arguments and stdin: `Ok(None)` means
/// this process is the launcher. The rule itself — which element triggers a
/// read, what a bad frame means — is the core's and is unit-tested there; what
/// this wrapper adds is only that the two inputs are *this* process's.
///
/// # Errors
///
/// None of its own: the rule is the core's, and this wrapper only supplies *this*
/// process's argv and stdin. Core's `adopt` reads stdin only when the element is
/// present, and treats a read that fails or a frame that does not decode as the
/// launcher case — `Ok(None)` — which inside a confinement fails closed at the
/// next grant or bind.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// let adopted = launch::adopt(std::env::args_os(), std::io::stdin().lock())?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    stdin: impl Read,
) -> Result<Option<Adopted>> {
    core_adopt(args, stdin)
}

/// Rebuild the listener the launch handed back as a descriptor.
///
/// **Once per process.** The rebuilt `TcpListener` is the sole owner of the
/// inherited handle ([`socket::from_value`]'s own safety argument), so a child
/// calls this once, on the descriptor its payload named: a second call with the
/// same value is two owners of one socket, and one of them will close it under
/// the other.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the launch named no
/// listener: a production launcher always binds one (design §10 step 6), so this
/// reports a launch that did not come from the launcher path rather than
/// fabricating a socket.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::Adopted;
/// use tinio_sandbox_windows::launch;
///
/// let adopted = Adopted::new(String::new(), Some(1040), Vec::new(), None, None);
/// let listener = launch::listener(&adopted)?;
/// // The child serves on the socket the launcher bound; it never binds one.
/// let _ = listener;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn listener(adopted: &Adopted) -> Result<TcpListener> {
    let fd = adopted.listener().ok_or_else(|| {
        launch(
            "rebuild the listener",
            "argv",
            io::Error::other("the launch named no listener descriptor"),
        )
    })?;
    socket::from_value(fd as u64)
}

/// Run `handler` when this process is stopped: the child-side half of the signal
/// relay (design §3 step 9, decision 20's second half).
///
/// **The delegation is the whole of this function, and the name is the reason
/// it exists.** The mechanism is [`crate::control::on_stop`]'s — a console
/// control handler that signals an event a thread of this process waits on — and
/// what this wrapper adds is the crate-level path the child side reaches for, the
/// one the facade carries under the same name so that `serve`'s child path can be
/// written once and name one function on all three platforms.
///
/// # Errors
///
/// Whatever [`crate::control::on_stop`] reports: the handler was refused, or the
/// event its thread waits on could not be created.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// // A forwarded stop lands here. It runs on a thread of this process's own, so
/// // what it does may allocate and take locks — which is what makes it the
/// // place a graceful shutdown starts.
/// launch::on_stop(|| {
///     let _ = ();
/// })?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn on_stop(handler: impl Fn() + Send + 'static) -> Result<()> {
    control::on_stop(handler)
}

/// Put this process back on the default disposition, so a relay armed for one
/// launch does not go on swallowing console control events after it.
///
/// The process that needs this is the one that outlives a launch — the test
/// harness, and any future CLI that serves twice — because `serve`'s launcher arm
/// forwards its child's status and exits instead (design §3 step 9).
///
/// # Errors
///
/// Whatever [`crate::control::disarm`] reports: the handler could not be removed,
/// which means it is still installed and still swallowing.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// launch::disarm()?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn disarm() -> Result<()> {
    control::disarm()
}

/// The child's arguments: the listener element first, the control elements next
/// when there are any, then the launcher's own arguments in order.
///
/// `args` is what follows `argv[0]` — `std::env::args_os().skip(1)` in
/// production, `Options::args` under test — and the program is not among them:
/// `command_line` writes this process's own executable as the child's `argv[0]`,
/// so the elements land at index 1 (and 2…) of that line and the launcher's
/// arguments keep their order after them (design §3 step 1: the child reaches the
/// same root and the same flags by the same path).
///
/// **The ordering is the contract, and the child parses its own arguments
/// against it.** The elements come first so that a child which drops `argv[0]`
/// and filters them is left with exactly the launcher's arguments — for a test
/// binary `child_role --exact` and nothing else — and so that neither launcher
/// plan nor the child's `own_args` has to guess where they sit. The listener's is
/// **always** present and always first: it is what `adopt` reads to decide this
/// process is a child at all.
///
/// **The launcher's side of that contract is `skip(1)` and nothing else.** A
/// caller that handed over its whole `args_os()` would put the launcher's own
/// executable path where the served root belongs — the child drops its `argv[0]`
/// and filters the element, so what is left is parsed as the launcher's
/// arguments.
///
/// `controls` is the inherited control channel's **instances**, for the launch that
/// carries them: the Windows launcher binds the pipes (only an unrestricted
/// process may, given the DACL the channel needs) and the child serves on every
/// handle it inherits. The same element is written once per instance — one prefix,
/// repeated — so the child's parse is unchanged and only the count differs. An
/// empty slice is every other launch, and the child's argv is then byte-for-byte
/// what it was before the control channel existed.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_windows::launch;
/// use windows_sys::Win32::Foundation::HANDLE;
///
/// let instances = [1044usize as HANDLE, 1048usize as HANDLE];
/// let args = launch::command_args(
///     1040usize as HANDLE,
///     &instances,
///     &[OsString::from("/srv/data"), OsString::from("--port")],
/// );
/// assert_eq!(args[0], "tinio-sandbox-fd=1040");
/// assert_eq!(args[1], "tinio-sandbox-control-fd=1044");
/// assert_eq!(args[2], "tinio-sandbox-control-fd=1048");
/// assert_eq!(
///     args[3..],
///     [OsString::from("/srv/data"), OsString::from("--port")]
/// );
///
/// // No control channel: the listener element and the launcher's arguments alone.
/// let bare = launch::command_args(
///     1040usize as HANDLE,
///     &[],
///     &[OsString::from("/srv/data"), OsString::from("--port")],
/// );
/// assert_eq!(bare[0], "tinio-sandbox-fd=1040");
/// assert_eq!(
///     bare[1..],
///     [OsString::from("/srv/data"), OsString::from("--port")]
/// );
/// ```
pub fn command_args(listener: HANDLE, controls: &[HANDLE], args: &[OsString]) -> Vec<OsString> {
    let mut child = Vec::with_capacity(args.len() + 1 + controls.len());
    child.push(listener_fd_arg(listener as i64));
    child.extend(
        controls
            .iter()
            .map(|control| control_fd_arg(*control as i64)),
    );
    child.extend(args.iter().cloned());
    child
}
