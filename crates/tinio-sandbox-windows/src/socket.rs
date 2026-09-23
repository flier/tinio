//! The inherited listener: the parent binds it, the child serves on it (design
//! decision 16).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14). A socket that crosses a process boundary is a raw handle, and rebuilding
//! a `TcpListener` from one is unsafe by construction.
//!
//! **It is plumbing, not a mechanism.** The handoff saves the child from binding
//! — it needs no bind to serve a socket it did not create — and nothing about the
//! confinement rests on it: an AppContainer's network policy is enforced by WFP
//! against the process's package SID, so a socket the parent created is subject
//! to exactly the same policy as one the child created itself. The design says so
//! in decision 16, and the LAN question the handoff leaves open — whether such a
//! listener reaches the network — is recorded by a later task, never assumed
//! here.

#![allow(unsafe_code)]

use std::{
    net::TcpListener,
    os::windows::io::{AsRawSocket, FromRawSocket},
};

use windows_sys::Win32::Foundation::HANDLE;

use crate::{
    _core::{Result, launch},
    handles::mark_inheritable as mark_handle_inheritable,
};

/// The handle value the launch payload carries (design decision 17).
///
/// A handle value survives inheritance unchanged — the probe's parent had 1040 and
/// the child saw 1040 — which is why the payload can name one at all.
///
/// # Examples
///
/// ```
/// use std::net::TcpListener;
///
/// use tinio_sandbox_windows::socket;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// assert!(socket::value(&listener) > 0);
/// ```
pub fn value(listener: &TcpListener) -> u64 {
    // `RawSocket` *is* `u64` on Windows, so this widens nothing: the value
    // below is what `handle` types as a pointer, not a conversion of it. The
    // cast an eye expects here is one `clippy::unnecessary_cast` reads as the
    // mistake it looks like, and this workspace builds with `-D warnings`.
    listener.as_raw_socket()
}

/// The listener as a raw handle, for the handle list the child inherits.
///
/// The same value as [`value`], typed the way `CreateProcessW` and
/// `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` want it: a socket *is* a handle on
/// Windows, which is why the listener can be passed at all (decision 16).
///
/// # Examples
///
/// ```
/// use std::net::TcpListener;
///
/// use tinio_sandbox_windows::socket;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// assert_eq!(socket::handle(&listener) as u64, socket::value(&listener));
/// ```
pub fn handle(listener: &TcpListener) -> HANDLE {
    listener.as_raw_socket() as HANDLE
}

/// Mark the listener inheritable.
///
/// Required, not defensive: Windows 8 and later create sockets non-inheritable,
/// so a handle list cannot name one without this call (design evidence table).
///
/// The handle it marks is [`handle`]'s, so the bit is set on the very value the
/// attribute list carries — a second spelling of the cast here is how the two
/// would drift apart.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when
/// `SetHandleInformation` refuses.
///
/// # Examples
///
/// ```no_run
/// use std::net::TcpListener;
///
/// use tinio_sandbox_windows::socket;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// socket::mark_inheritable(&listener)?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn mark_inheritable(listener: &TcpListener) -> Result<()> {
    mark_handle_inheritable(handle(listener))
        .map_err(|source| launch("mark the listener inheritable", "listener", source))
}

/// Rebuild the listener from the handle value a launcher passed.
///
/// The handle is trusted rather than validated: only a launcher can write the
/// payload that names it (design decision 17), and the child that calls this is
/// the process that inherited it.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when Winsock cannot be
/// initialised — the only thing here that can fail.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::socket;
///
/// let listener = socket::from_value(1040)?;
/// // The child serves on the socket the launcher bound; it never binds one.
/// let _ = listener;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn from_value(raw: u64) -> Result<TcpListener> {
    initialize_winsock()?;
    // SAFETY: this is the confined child's path. The handle came from the launch
    // payload the launcher wrote, this process inherited it, and nothing else
    // here has touched it, so the value is a live listening socket no other
    // value in this process owns.
    Ok(unsafe { TcpListener::from_raw_socket(raw) })
}

/// Trigger Rust's lazy Winsock initialisation.
///
/// `std` initialises Winsock on the first socket it *creates*, and a rebuilt
/// socket is not created by `std`: without this, the first Winsock call on it
/// fails with `WSANOTINITIALISED`. **Measured, not reasoned**: with this call
/// removed the child's `accept` on the inherited socket returns os error
/// `10093` (`WSANOTINITIALISED`) while the rebuild itself still succeeds, which
/// is the same failure the confined child would have met on its first accept.
/// One loopback bind-and-drop is enough, and a loopback bind needs no
/// capability inside the container (design evidence table).
fn initialize_winsock() -> Result<()> {
    TcpListener::bind(("127.0.0.1", 0))
        .map(drop)
        .map_err(|source| launch("initialise Winsock", "127.0.0.1:0", source))
}
