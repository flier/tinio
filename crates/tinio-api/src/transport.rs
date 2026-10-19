//! Serving the router over the transports the config selected.
//!
//! **This module is where the bind's runtime-free listeners become tokio's.** Task 2 created
//! `std` ones (and, on Windows, a raw pipe handle) so `Plane::bind` could be the
//! single-instance lock before the CLI's runtime existed; `from_std` and
//! `NamedPipeServer::from_raw_handle` both need a reactor, and `serve_transport` runs inside
//! the runtime, so the conversions live here and nowhere else (`Channel` never enters tokio's
//! types).
//!
//! The unix socket is an axum `Listener` once converted, so `axum::serve` drives it directly,
//! with `with_graceful_shutdown` on the plane's channel — ceasing to accept is the plane's
//! job, and the ≤ 10 s bound on the drain is the CLI's (decision 12). The Windows named pipe is
//! not a `Listener` at all: `NamedPipeServer` has no impl, and the pipe's lack of TCP's
//! half-close makes keep-alive unsafe there, so it gets the manual connection loop
//! `research.md:99` prescribes. TCP and TLS (Task 5, Task 6) are `Listener`s again.

#[cfg(any(unix, feature = "tls"))]
use std::fs;
#[cfg(any(windows, feature = "tls"))]
use std::io;
#[cfg(unix)]
use std::os::unix::{fs::MetadataExt, net::UnixListener};
#[cfg(windows)]
use std::os::windows::io::{OwnedHandle, RawHandle};
// `tls` bounds the handshake; `windows` bounds the pipe drain ([`DRAIN_LIMIT`]). One import for
// both, because the default graph has both and an `use` twice over does not compile.
#[cfg(any(feature = "tls", windows))]
use std::time::Duration;
// The named-pipe pool's own imports: the stream wrapper behind the per-connection bridge.
#[cfg(windows)]
use std::{
    future::poll_fn,
    pin::Pin,
    sync::MutexGuard,
    task::{Context as TaskContext, Poll},
};
use std::{
    mem,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process,
    sync::{Arc, Mutex, PoisonError},
};

#[cfg(feature = "tls")]
use axum::serve::Listener;
#[cfg(windows)]
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixListener as TokioUnixListener;
#[cfg(feature = "tls")]
use tokio::time::sleep;
// **Two `#[cfg]`s need this name, and the `any` is load-bearing.** The Windows arm uses it in
// `wait_for_peer_close`, and the TLS arm uses it for the handshake bound — and the TLS arm is
// `#[cfg(feature = "tls")]` with **no** platform condition, so on macOS and Linux this import
// must be here for the TLS build. Gated on `windows` alone it is absent exactly where the
// handshake's `timeout` is compiled, which is a hard `E0425` for every non-Windows TLS build
// (measured on macOS 15.8, 2026-09-25: `cargo build -p tinio` fails in this file at line 1051).
// `tls` is a default feature of this crate, so that was every default build on those targets; it
// stayed invisible because the Windows and macOS legs each satisfied one half of the old gate.
#[cfg(any(windows, feature = "tls"))]
use tokio::time::timeout;
#[cfg(windows)]
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::windows::named_pipe::NamedPipeServer,
    task::JoinSet,
};
use tokio::{net::TcpListener as TokioTcpListener, sync::watch};
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig, server::TlsStream};
#[cfg(windows)]
use windows_sys::Win32::Foundation::{ERROR_BROKEN_PIPE, ERROR_NO_DATA};

#[cfg(feature = "lifecycle")]
use crate::lifecycle::LifecycleAdmin;
#[cfg(windows)]
use crate::pipe;
use crate::{
    Error,
    bind::{self, Channel},
    metrics::Gauges,
    router::{Auth, Context, Shared, router},
    state::{State, remove_if_present},
};

/// The upper bound on [`wait_for_peer_close`]: how long one instance keeps waiting for a client
/// that is still attached **after its response has been written**, before the instance is
/// force-reclaimed.
///
/// **The waiting it bounds is at kernel speed, and the bound is ~1 100× its worst measurement.**
/// On a client that hangs up after reading, which is every client this crate ships, the wait ends
/// the moment the client's handle closes: measured over 201 sequential dials on this branch's host
/// (`the_pipe_pool_serves_back_to_back_dials_with_no_gap`), 83 µs to 222 µs — p50 95 µs, p99
/// 182 µs, max 222 µs, re-measured 2026-09-28 with the whole-chunk read below (the earlier one-byte
/// read measured 80–181 µs, p50 97 µs, p99 177 µs: the distribution is the wait for the close, not
/// the size of the read). A quarter of a second is three orders of magnitude above the mean, so a
/// **healthy** client cannot reach this bound, whatever the scheduler does to it.
///
/// **What the bound buys, and what it costs.** A client that completes an exchange and then holds
/// its handle open — measured with both instances wedged, `two_stalled_clients_do_not_deafen_the_name`
/// — otherwise parks that instance forever: `DisconnectNamedPipe` is only safe after the peer is
/// gone, so the loop waits, and with `bind::INSTANCES` of them wedged the *name* goes deaf, which
/// no retry can repair. At the limit the instance is disarmed anyway, and that is what cannot be
/// lost: the response has already been **written**. `tokio`'s write on a pipe handle is a
/// `WriteFile` into the pipe's buffer — `poll_flush` is a no-op there (`NamedPipeServer`'s
/// `poll_flush` answers `Ready` without touching the handle, `tokio-1.53.1`'s
/// `net/windows/named_pipe.rs:918`; the `FlushFileBuffers` an earlier version of this note claimed
/// was measured false on 2026-09-28 by a client that read nothing — its answer was still in the
/// pipe, whole, when the client finally read it) — so what a wedged client holding the connection
/// open is sitting on is that buffered response; what it cannot take is anything after it, because
/// `keep_alive(false)` means there is nothing after it, and what it is holding is the
/// *connection*. So the bytes the disarm discards are at most what that client chose not to read,
/// and it is the client that is wrong, not the response that is lost.
///
/// **What the disarm does not discard is the runtime's own read state, and the limit is answered
/// with that rather than with the disarm alone.** Measured on 2026-09-28: the read the dropped wait
/// leaves in flight answers for the *next* connection on the same instance — one byte of its
/// request, or the `ERROR_PIPE_NOT_CONNECTED` the disarm gives it — and a request whose first byte
/// went that way reaches the router as `ET /status HTTP/1.1`, refused `405 Method Not Allowed`.
/// [`wait_for_peer_close`] carries what was done about it and why the two ways of throwing that
/// state away are closed, and [`Pool::poll_read`] is where the connection that inherits it is told.
///
/// **It is not a request timeout and must not become one.** The wait starts *after* hyper has
/// returned the connection, so no request is in flight inside it; a request that is slow to be
/// answered is slow in the handler, which this constant does not touch. The plane's own shutdown
/// stays the CLI's ≤ 10 s bound (decision 12) — this is a per-instance liveness backstop under it.
#[cfg(windows)]
const DRAIN_LIMIT: Duration = Duration::from_millis(250);

/// Serves `shared` over a bound local channel until `stop` flips, then drains and returns.
///
/// The channel is self-describing: the Windows form carries its own name (`Channel { name, .. }`
/// there), so no caller passes an identity in and neither platform's branch re-derives one — the
/// same name [`Channel::identity`] reports is what `state.control_name` records.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(unix)]
/// # {
/// use std::{path::Path, sync::Arc};
///
/// use tinio_api::{
///     bind::bind_unix,
///     router::{Auth, Context},
///     state::State,
///     transport::serve_local,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// let path = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
/// let state = State::new(path.to_string_lossy(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, stop));
/// let channel = bind_unix(path)?;
/// serve_local(channel, shared, rx).await
/// # }
/// # }
/// ```
pub async fn serve_local(
    channel: Channel,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        // The channel's own fields, destructured: on this platform the struct is the whole type,
        // so there is no variant to match on and no unreachable arm to write.
        let Channel { listener, .. } = channel;
        // The one conversion: `std` (bound before the runtime) to tokio (served inside it).
        let listener = TokioUnixListener::from_std(listener).map_err(Error::Io)?;
        axum::serve(listener, router(shared))
            .with_graceful_shutdown(until_stopped(stop))
            .await
            .map_err(|err| Error::Transport(format!("unix channel: {err}")))
    }
    #[cfg(windows)]
    {
        // The pipe form carries its own name and its whole pool, so the identity is passed on
        // rather than re-derived and no instance is created here.
        let Channel { name, handles } = channel;
        serve_pipe(handles, &name, shared, stop).await
    }
}

/// The shape every `with_graceful_shutdown` call in this plan takes: resolve when the plane's
/// channel flips (or when its sender is gone, which is a shutdown too).
///
/// `serve_local`'s unix arm and `serve_tcp` are its callers on every platform; `serve_https` is
/// the third (Task 6), so all three transports stop on the plane's one channel.
pub(crate) async fn until_stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.changed().await;
}

/// The named-pipe accept side: **one accept loop per bound instance**, HTTP/1 keep-alive off, and
/// the shutdown channel watched while no client is connecting.
///
/// **The pool is the name's liveness, and the pool comes from the bind or from a launcher.**
/// Every client dials the one name, so it can only reach an instance that is *listening*: the
/// channel is served by the instances `bind_pipe` created, and while one of them serves a
/// connection another is still waiting for the next client. That is what makes a second
/// `status` after a first answer rather than `ERROR_PIPE_BUSY`.
///
/// **Nothing here creates an instance, and that is the point.** Inside an AppContainer this
/// process's identity is the package SID, so `CreateNamedPipeW` for an instance of the channel's
/// name is refused `ERROR_ACCESS_DENIED`: the pipe carries Windows' own default named-pipe
/// descriptor, which names the launcher's own user and not the package SID ([`pipe::create_pipe`]),
/// and a child
/// that made its own successor would answer once and then stop listening. The unconfined bind creates
/// the pool ([`crate::bind::bind_pipe`]) and a confined launcher creates it and hands every
/// handle over ([`Plane::adopt`]), so this loop only ever serves instances that already exist.
///
/// **Reusing one instance was measured and rejected.** `DisconnectNamedPipe` then
/// `ConnectNamedPipe` on the instance in hand leaves a tick in which it is neither connected nor
/// listening; because `client::exchange` returns as soon as it has the response while the
/// server's disarm runs later, a client's *next* dial lands in that tick and is killed
/// mid-request — measured as `Transport("/status: connection error")`, the same "Pipe is broken"
/// the confined run reports, where this rotation reports zero retries over two hundred dials.
#[cfg(windows)]
async fn serve_pipe(
    handles: Vec<OwnedHandle>,
    identity: &str,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    if handles.is_empty() {
        return Err(Error::Transport(
            "the control channel was bound with no instances: nothing could ever answer on it"
                .to_string(),
        ));
    }
    // Built once, below the loops: nothing about the router varies per instance, and its clone is
    // an `Arc` bump (`axum::Router` keeps its state behind one), so a per-instance rebuild would
    // re-derive what this one already holds.
    let app = router(shared);
    // One task per instance, and the set is also the drain: a stop ends the accept side, and
    // every connection already accepted runs to completion before this returns.
    let mut listeners = JoinSet::new();
    for handle in handles {
        // The raw handle becomes tokio's own type here, inside the runtime — the constructor
        // needs the reactor, which is why the bind could only hand over an `OwnedHandle`
        // (`pipe` is the crate's one raw-Windows module, Task 2's note).
        let pipe = pipe::adopt_pipe(handle).map_err(Error::Io)?;
        listeners.spawn(pipe_instance_loop(
            pipe,
            app.clone(),
            stop.clone(),
            identity.to_string(),
        ));
    }

    while let Some(served) = listeners.join_next().await {
        match served {
            Ok(Ok(())) => {}
            // One instance's accept loop failing is the channel failing: the pool exists to keep
            // the name answering, and a loop that cannot `connect` any more is that guarantee
            // gone. The rest are aborted so the plane reports one error rather than hanging on
            // instances that are still waiting for clients that will never be served faithfully.
            Ok(Err(err)) => {
                listeners.shutdown().await;
                return Err(err);
            }
            Err(err) => {
                listeners.shutdown().await;
                return Err(Error::Internal(format!(
                    "a management pipe instance loop: {err}"
                )));
            }
        }
    }
    Ok(())
}

/// One instance's whole life: wait for a client, serve it to the end, disarm, wait again.
///
/// **One task per instance is what preserves the drain.** A stop observed while this task is
/// *waiting* ends it; a stop observed while it is *serving* cannot, because the connection is not
/// a branch of the `select!` — so the client that got in before the shutdown is still answered
/// ([`the_pipe_pool_serves_back_to_back_dials_with_no_gap`]'s open-at-stop half is the assertion),
/// which is the same property the previous single-instance loop got from its `JoinSet`.
///
/// **An instance whose drain did not see a clean close carries the previous connection's runtime
/// state, and that is what the loop carries forward.** [`serve_pipe_connection`] answers with it,
/// and it is handed to the *next* connection's [`Pool`], which is the one place the difference can
/// still be told apart — before that connection has read a byte, an error off the instance is the
/// association's answer rather than the client's ([`Pool::poll_read`] carries the measurement and
/// the retry). The two mechanisms that would have thrown the state away instead are both refused
/// by the platform on this crate's own serving processes, and [`wait_for_peer_close`]'s doc has
/// both.
#[cfg(windows)]
async fn pipe_instance_loop(
    mut pipe: NamedPipeServer,
    app: axum::Router,
    mut stop: watch::Receiver<bool>,
    identity: String,
) -> Result<(), Error> {
    // Whether the connection that just ended left this instance carrying the runtime's own answer
    // — its drain's report, spent by the connection that follows.
    let mut stale = false;
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            connected = pipe.connect() => {
                connected.map_err(|err| Error::Transport(format!("{identity}: pipe connect: {err}")))?;
            }
        }
        (pipe, stale) = serve_pipe_connection(pipe, stale, app.clone(), &identity).await?;
    }
    Ok(())
}

/// Serves **one** connection on `pipe`, drains it, and gives the instance back to its loop.
///
/// **The disarm is why the instance comes back at all, and the drain is why the disarm is safe.**
/// `DisconnectNamedPipe` discards whatever the instance still holds unread, while
/// `keep_alive(false)` lets hyper return as soon as it has *written* the response — not when the
/// client has read it. Disarming straight after the connection therefore destroys responses under
/// load: measured on this host, the first two dials succeed and every dial after them fails its
/// exchange while the server accepts and serves each one. [`wait_for_peer_close`] is the fix, and
/// it is not optional.
///
/// The handover is forced by `hyper`: `serve_connection` consumes its stream and bounds it
/// `Io: 'static`, while tokio implements `AsyncRead`/`AsyncWrite` for `NamedPipeServer` itself
/// rather than for `&NamedPipeServer` — so the connection owns the instance through [`Pool`] and
/// its end is where the loop gets it back.
///
/// **The pair it answers with is the instance and whether its drain saw a clean close.** The
/// second half is not diagnostics: a drain that ended without the client's close hands the
/// instance back carrying an answer of its own — the read the disarm cancelled, or the bytes the
/// drain's own read took — and the connection that follows is the one that has to be told
/// ([`Pool::poll_read`]).
///
/// `stale` is the *previous* connection's report, which this one's stream is built with; the
/// answer returned is this connection's own (see [`wait_for_peer_close`]).
#[cfg(windows)]
async fn serve_pipe_connection(
    pipe: NamedPipeServer,
    stale: bool,
    app: axum::Router,
    identity: &str,
) -> Result<(NamedPipeServer, bool), Error> {
    // `http1` borrows the builder mutably (`hyper-util`'s `auto::Builder::http1(&mut self)`), so
    // the binding is `mut`.
    let mut builder = auto::Builder::new(TokioExecutor::new());
    // axum's `Router` is a tower service and this loop is hyper's own, so the bridge is explicit —
    // `serve_connection` wants `hyper::service::Service`, and axum wraps with exactly this adapter
    // in its own server (`axum-0.8.9/src/serve/mod.rs:388`).
    let service = TowerToHyperService::new(app);
    // The stream the connection is served on: `hyper`'s bridge over [`Pool`], which owns the
    // instance for the connection's life. The slot this frame shares with the stream is what lets
    // the instance come back out once the connection is over.
    let io = TokioIo::new(Pool::new(pipe, stale));
    let slot = io.inner().slot();
    if let Err(err) = builder
        .http1()
        // The pipe has no half-close, so keep-alive would let a client wait forever for a
        // response delimitation TCP would have provided (`research.md:99`).
        .keep_alive(false)
        .serve_connection(io, service)
        .await
    {
        // A connection-level failure is the client's, not the channel's: the instance is still
        // returned below and the instance's next `connect` works.
        tracing::debug!(%identity, error = %err, "management pipe connection ended");
    }
    let mut pipe = Pool::release(slot).ok_or_else(|| {
        Error::Internal("the connection's stream still holds the pipe instance".to_string())
    })?;
    // Before the disarm, and never after it: this is the drain that keeps the response the client
    // has not read yet from being thrown away (see this function's doc).
    let clean = wait_for_peer_close(&mut pipe, identity).await;
    // Back to the listening state, so this instance's next `connect` can accept. `ERROR_NO_DATA`
    // is the client having hung up first: the instance is disconnected either way, which is the
    // state the next `connect` wants, so it is not a failure.
    if let Err(err) = pipe.disconnect()
        && err.raw_os_error() != Some(ERROR_NO_DATA as i32)
    {
        return Err(Error::Transport(format!(
            "{identity}: pipe disconnect: {err}"
        )));
    }
    Ok((pipe, !clean))
}

/// Waits until the client's end is gone, so the [`DisconnectNamedPipe`] that follows has nothing
/// left to discard — **bounded by [`DRAIN_LIMIT`]**, after which the instance is reclaimed anyway
/// and the reclaim is reported as an event. Answers **whether the client's close was the thing
/// that ended the wait**, which is not the same as "the wait ended": a client that writes after
/// its response ends the wait without ever closing, and an instance that comes back from either
/// that or the bound is one whose next connection has to be told ([`serve_pipe_connection`]).
///
/// A zero-length read is `Ok` in tokio's `poll_read` spelling; the broken-pipe error is the same
/// news on an abrupt close, and any other failure is the client's own teardown. The wait is
/// wrapped in a `timeout` rather than given a deadline of its own so the cancellation is the
/// usual one — and **that cancellation is not free, which is what the answer is about**: the
/// dropped read is neither cancelled in the kernel nor cancelled in the runtime's read state, it
/// merely stops being ours. Measured on this host: that read goes on to answer for the *next*
/// connection — one byte of its request, or the `ERROR_PIPE_NOT_CONNECTED` the disarm gives it —
/// and the request then reaches the router as `ET /status HTTP/1.1`, refused `405 Method Not
/// Allowed`.
///
/// **Neither of the two mechanisms that would retire the state instead is available**, and both
/// were measured or are written down rather than assumed. A *successor* instance — close this one
/// and create the next at the same name — is refused to the process that serves these: inside a
/// container its identity is the package SID and `CreateNamedPipeW` on the launcher's pipe, whose
/// descriptor is Windows' own default named-pipe descriptor rather than one naming the package SID,
/// answers `ERROR_ACCESS_DENIED` (`pipe::adopted_pipe`'s note; `bind::INSTANCES` says the same
/// about the name going deaf for good). And *renewing* the instance in place — duplicate the
/// handle, drop the association that holds the state, wrap the duplicate — is refused by the
/// kernel: the duplicate makes `DuplicateHandle` succeed, but the fresh association's
/// registration is `CreateIoCompletionPort` on an object that is already associated with the
/// completion port, and that answers `ERROR_INVALID_PARAMETER (87)` (measured on this host
/// 2026-09-28, the two steps apart so the error's site was not inferred). What is left is the
/// state itself: the read above takes **a whole chunk**, never the one byte that would leave a
/// request's tail behind in it, and the connection after it is the one told about the rest.
///
/// A client that connects and never lets go holds this instance **for at most the limit** — the
/// measurement and the argument that nothing it could still be owed is discarded are on
/// [`DRAIN_LIMIT`]. The other instances of the pool keep listening while this one waits, which is
/// still the property the pool exists for; the bound is what keeps the *name* from going deaf
/// when every instance is held at once.
///
/// **The event is the observability, and it is `warn` on purpose.** A forced reclaim means a
/// client is sitting on a connection it has already been answered on — a client bug or a wedged
/// process, never normal traffic — so it is worth a line in the log rather than a debug detail,
/// and it costs one event on a path that is otherwise silent.
///
/// [`DisconnectNamedPipe`]: https://learn.microsoft.com/windows/win32/api/namedpipeapi/nf-namedpipeapi-disconnectnamedpipe
#[cfg(windows)]
async fn wait_for_peer_close(pipe: &mut NamedPipeServer, identity: &str) -> bool {
    use std::time::Instant;

    let started = Instant::now();
    // **A whole chunk, not one byte**: the read is what takes the client's trailing bytes away,
    // and one byte of a chunk is what leaves the rest of it in the runtime's buffer for the next
    // connection to read as its request. 4 KiB is the runtime's own chunk (`mio`'s named-pipe
    // reads go through a buffer of exactly that size, `mio-1.2.3/src/sys/windows/named_pipe.rs`'s
    // `get_buffer(4 * 1024)`), so a read this size empties it whole.
    let mut buf = [0u8; 4096];
    let closed = timeout(
        DRAIN_LIMIT,
        poll_fn(|cx| {
            let mut read = ReadBuf::new(&mut buf);
            match Pin::new(&mut *pipe).poll_read(cx, &mut read) {
                // The client's close: nothing was delivered, so the instance is in step.
                Poll::Ready(Ok(())) if read.filled().is_empty() => Poll::Ready(true),
                // A chunk of the client still being there. One read per poll, so a client that
                // keeps writing cannot hold this loop: what is taken is bounded, the disarm
                // discards the rest, and the answer says the close was not seen.
                Poll::Ready(Ok(())) => Poll::Ready(false),
                Poll::Ready(Err(err)) if err.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) => {
                    Poll::Ready(true)
                }
                Poll::Ready(Err(_)) => Poll::Ready(true),
                Poll::Pending => Poll::Pending,
            }
        }),
    )
    .await;
    match closed {
        // The client's close, the only clean end of a wait.
        Ok(true) => true,
        // The client wrote after its response: the bytes are taken, and the next connection is
        // told the instance's state is not trustworthy (`Pool::poll_read`).
        Ok(false) => false,
        Err(_) => {
            tracing::warn!(
                %identity,
                limit = ?DRAIN_LIMIT,
                waited = ?started.elapsed(),
                "the client did not close after its response; reclaiming the pipe instance"
            );
            false
        }
    }
}

/// The instance one connection is served on, **handing it back when the connection ends**.
///
/// **Every part of this shape is forced by `serve_connection`.** It takes its stream by value and
/// bounds it `Io: 'static`, so the instance cannot be borrowed from a frame that must use it again
/// — the connection has to own it. It also consumes the stream, so the owner cannot read the value
/// back out of the same binding afterwards. So the instance travels in a slot both that frame and
/// the connection hold ([`Arc`]), the connection's end drops the stream and leaves the instance in
/// the slot, and [`Pool::release`] is the frame taking it back out.
///
/// **The traits are tokio's, not `hyper`'s.** tokio implements `AsyncRead`/`AsyncWrite` for
/// `NamedPipeServer` itself, so the value in the slot is one, and `hyper_util::rt::TokioIo` is the
/// one bridge to `hyper`'s traits that `serve_connection` wants — putting the slot behind a second
/// `TokioIo` would mean going tokio -> `TokioIo` -> tokio -> `TokioIo`, which is why this wrapper
/// implements the tokio side and lets one `TokioIo` sit on top of it.
///
/// **It is also where a connection is told that its instance came back from a drain that did not
/// see a clean close**, which is the one place that difference is still tellable: the runtime
/// reads a pipe through a per-handle state machine that the disarm does not clear, so the first
/// read of such a connection can be answered by the *previous* connection's read — the
/// `ERROR_PIPE_NOT_CONNECTED` its cancellation produced, or, before the drain took whole chunks,
/// the tail of a request (`[`Pool::poll_read`] has the measurement). A connection that has not
/// read a byte yet can tell that apart from the client's own teardown, and does; a later error is
/// the client's, as it always was.
#[cfg(windows)]
struct Pool {
    /// `Some` until the connection's stream is dropped; `None` only once it has been taken out.
    slot: Arc<Mutex<Option<NamedPipeServer>>>,
    /// Whether the instance this connection is served on came back from a drain that did not see
    /// the client's close ([`serve_pipe_connection`]'s pair).
    stale: bool,
    /// Whether this connection has read a byte of its own yet: the retry below is for the state a
    /// connection *inherits*, and a connection that has already been served is past that.
    delivered: bool,
}

#[cfg(windows)]
impl Pool {
    /// Wraps `inner` for one connection, which `stale` marks as one whose drain did not see the
    /// client's close.
    fn new(inner: NamedPipeServer, stale: bool) -> Self {
        Self {
            slot: Arc::new(Mutex::new(Some(inner))),
            stale,
            delivered: false,
        }
    }

    /// The shared slot, for the frame that outlives the connection.
    fn slot(&self) -> Arc<Mutex<Option<NamedPipeServer>>> {
        Arc::clone(&self.slot)
    }

    /// Closes the slot and gives the instance back: the last `Arc` handle, with the connection's
    /// stream already dropped, is the one that can. `None` means the stream outlived the
    /// connection, which its `await` rules out.
    fn release(slot: Arc<Mutex<Option<NamedPipeServer>>>) -> Option<NamedPipeServer> {
        Arc::try_unwrap(slot).ok()?.into_inner().ok()?.take()
    }

    /// The inner stream, for the poll that is running now. A poisoned lock is recovered rather
    /// than unwrapped: the guard is held only across a short invariant-preserving mutation, so
    /// its poison flag cannot describe a half-written slot (style.md §Panics, D3).
    fn held(&self) -> MutexGuard<'_, Option<NamedPipeServer>> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `body` against the instance in the slot, with the guard taken and given back around it —
    /// **the one spelling of the forwarded-poll shape**, so the four `AsyncWrite` methods and the
    /// read retry below cannot disagree about it.
    ///
    /// An empty slot is answered as this connection's own I/O error rather than a panic: the
    /// instance is taken out only by [`Pool::release`], on the frame holding the other `Arc`, so
    /// an empty slot means this connection outlived the instance it was served on — a connection
    /// to end, not a process to take down.
    fn with_instance<T>(
        &mut self,
        body: impl for<'a> FnOnce(Pin<&'a mut NamedPipeServer>) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match self.held().as_mut() {
            Some(instance) => body(Pin::new(instance)),
            None => Poll::Ready(Err(io::Error::other(
                "the connection's pipe instance is no longer in its slot",
            ))),
        }
    }
}

#[cfg(windows)]
impl AsyncRead for Pool {
    /// **The first read of a connection whose instance came back from an unclean drain is retried
    /// once, and the retry is the whole of the fix for it.** Measured on this host, the drain's
    /// abandoned read answers for the connection that follows — `ERROR_PIPE_NOT_CONNECTED`, the
    /// error the disarm gives the read it cancels — and with that error handed to it hyper ends the
    /// connection before reading a byte, so a dial that should have been answered `200` was
    /// answered `Transport("/status: connection error")` and the dials after it were served a
    /// previous request's tail. One retry is enough because the runtime's own read of an error
    /// schedules the next one, and that next read is the one that carries the client's request: by
    /// the time the second poll runs, the state the connection inherited has been spent.
    ///
    /// **It cannot hide anything that was not already lost**: the retry runs only while this
    /// connection has read no byte, and only on an instance the caller marked (a clean drain marks
    /// nothing, which is why the back-to-back dials leg still sees `0` retries at the dial level —
    /// this retry is inside one connection and never a second dial). A client that really has gone
    /// answers the retry with the same error, or with the close, and hyper ends the connection
    /// exactly as it would have.
    ///
    /// **What this does not fix, and what was measured rather than assumed**: the runtime's read
    /// state can also hold *bytes* the previous connection never read — a client that writes after
    /// its response and never closes leaves its tail in the buffer (the drain's read takes a whole
    /// chunk now, which is what keeps the drain itself from being the one that leaves it, but a
    /// client that writes more than a chunk still can). Bytes have no marker that tells them from
    /// a request, so no connection can tell those apart, and the two mechanisms that would retire
    /// them — a successor instance, or the same instance renewed — are refused to the confined
    /// server ([`wait_for_peer_close`]'s doc has both).
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        let before = buf.filled().len();
        let out = poll_instance(this, cx, buf);
        let out = match out {
            Poll::Ready(Err(_)) if this.stale && !this.delivered => poll_instance(this, cx, buf),
            decided => decided,
        };
        this.delivered |= buf.filled().len() != before;
        out
    }
}

/// One poll of the instance in `pool`'s slot, with the guard taken and given back around it.
#[cfg(windows)]
fn poll_instance(
    pool: &mut Pool,
    cx: &mut TaskContext<'_>,
    buf: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>> {
    pool.with_instance(|instance| instance.poll_read(cx, buf))
}

#[cfg(windows)]
impl AsyncWrite for Pool {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .with_instance(|instance| instance.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .with_instance(|instance| instance.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .with_instance(|instance| instance.poll_shutdown(cx))
    }

    /// Forwarded rather than left to the default, which buffers: the pipe has a vectored write of
    /// its own, and one copy of a response is enough.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .with_instance(|instance| instance.poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        self.held()
            .as_ref()
            .is_some_and(AsyncWrite::is_write_vectored)
    }
}

/// The local channel's platform form, as the caller resolved it.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_api::transport::LocalForm;
///
/// let unix = LocalForm::Unix(PathBuf::from("/run/tinio/tinio-0123456789abcdef.sock"));
/// let pipe = LocalForm::Pipe(r"\\.\pipe\tinio-mine".to_string());
/// assert!(matches!(unix, LocalForm::Unix(_)));
/// assert!(matches!(pipe, LocalForm::Pipe(_)));
/// ```
#[derive(Debug, Clone)]
pub enum LocalForm {
    /// A unix socket path.
    Unix(PathBuf),
    /// A Windows pipe name.
    Pipe(String),
}

/// Bind the local control channel `form` names.
///
/// The unix and pipe binds stay in this crate. A caller does not match the
/// variant to pick a platform function.
///
/// # Errors
///
/// The bind failed, or `form` is the other platform's variant.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::transport::{LocalForm, bind_local};
///
/// let channel = bind_local(&LocalForm::Unix(PathBuf::from("/run/tinio/tinio.sock")))?;
/// assert!(!channel.identity().is_empty());
/// # Ok::<(), tinio_api::Error>(())
/// ```
pub fn bind_local(form: &LocalForm) -> Result<Channel, Error> {
    match form {
        LocalForm::Unix(path) => {
            #[cfg(unix)]
            {
                bind::bind_unix(path)
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport(
                    "a unix control channel cannot be bound on this platform".into(),
                ))
            }
        }
        LocalForm::Pipe(name) => {
            #[cfg(windows)]
            {
                bind::bind_pipe(name)
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport(
                    "a named-pipe control channel cannot be bound on this platform".into(),
                ))
            }
        }
    }
}

/// The HTTPS settings, present only when the transport is HTTPS.
///
/// # Examples
///
/// ```rust
/// use std::{net::SocketAddr, path::PathBuf};
///
/// use tinio_api::transport::HttpsOptions;
///
/// let options = HttpsOptions {
///     addr: "127.0.0.1:9443".parse().expect("addr"),
///     cert: PathBuf::from("/etc/tinio/cert.pem"),
///     key: PathBuf::from("/etc/tinio/key.pem"),
/// };
/// assert_eq!(options.addr, SocketAddr::from(([127, 0, 0, 1], 9443)));
/// assert!(options.cert.ends_with("cert.pem"));
/// ```
#[derive(Debug, Clone)]
pub struct HttpsOptions {
    /// The bind address.
    pub addr: SocketAddr,
    /// The PEM certificate chain.
    pub cert: PathBuf,
    /// The PEM private key.
    pub key: PathBuf,
}

/// What the caller asked for, before exclusivity is applied.
///
/// # Examples
///
/// ```rust
/// use tinio_api::transport::Selection;
///
/// // The CLI's starting point: nothing selected, which `select` refuses.
/// let empty = Selection::default();
/// assert!(empty.local.is_none() && empty.http.is_none() && empty.https.is_none());
/// ```
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// The local channel, when enabled.
    pub local: Option<LocalForm>,
    /// TCP HTTP, when its section or a `--api http://…` flag enabled it.
    pub http: Option<SocketAddr>,
    /// TCP HTTPS, when its section plus cert/key or a `--api https://…` flag enabled it.
    pub https: Option<HttpsOptions>,
}

/// A bound transport, ready to serve.
///
/// # Examples
///
/// ```no_run
/// use std::net::SocketAddr;
///
/// use tinio_api::{
///     router::Auth,
///     transport::{HttpsOptions, Selection, select},
/// };
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// let http = select(Selection {
///     http: Some("127.0.0.1:9001".parse::<SocketAddr>().expect("addr")),
///     ..Selection::default()
/// })?;
/// assert_eq!(http.auth(), Auth::Loopback);
/// assert!(http.identity().starts_with("127.0.0.1:"));
///
/// // HTTPS is the arm the `tls` feature compiles in; without the feature, `select`
/// // refuses the same selection instead.
/// # #[cfg(feature = "tls")]
/// # {
/// let https = select(Selection {
///     https: Some(HttpsOptions {
///         addr: "127.0.0.1:9443".parse().expect("addr"),
///         cert: "/etc/tinio/cert.pem".into(),
///         key: "/etc/tinio/key.pem".into(),
///     }),
///     ..Selection::default()
/// })?;
/// assert_eq!(https.identity(), "127.0.0.1:9443");
/// # }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Transport {
    /// The local channel.
    Local {
        /// The bound channel, which is also what `state` records: [`Channel::identity`] is the
        /// control name, and the token rule is always [`Auth::Local`].
        channel: Channel,
    },
    /// TCP HTTP.
    Tcp {
        /// The bound listener — `std`, converted to tokio's inside `serve_tcp`, because
        /// `select` runs before the runtime exists.
        listener: TcpListener,
        /// The bound address.
        addr: SocketAddr,
    },
    /// TCP HTTPS (feature `tls`).
    #[cfg(feature = "tls")]
    Https {
        /// The bound listener — `std`, as in the `Tcp` arm.
        listener: TcpListener,
        /// The bound address.
        addr: SocketAddr,
        /// The PEM certificate chain.
        cert: PathBuf,
        /// The PEM private key.
        key: PathBuf,
    },
}

/// Applies the three-choose-one rule and binds the winner.
///
/// A local arm keeps the **bound** channel, never the requested one: [`Transport::identity`] is
/// [`Channel::identity`], so what the bind created is what `state` records and what every client
/// dials. `Plane::bind` is what guarantees that identity is absolute.
///
/// # Examples
///
/// ```no_run
/// use tinio_api::{
///     router::Auth,
///     transport::{Selection, select},
/// };
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// // Exactly one: none is a startup error, and two are a usage error.
/// assert!(select(Selection::default()).is_err());
///
/// let transport = select(Selection {
///     http: Some("127.0.0.1:9001".parse().expect("addr")),
///     ..Selection::default()
/// })?;
/// assert_eq!(transport.identity(), "127.0.0.1:9001");
/// assert_eq!(transport.auth(), Auth::Loopback);
/// # Ok(())
/// # }
/// ```
pub fn select(selection: Selection) -> Result<Transport, Error> {
    let enabled = usize::from(selection.local.is_some())
        + usize::from(selection.http.is_some())
        + usize::from(selection.https.is_some());
    if enabled == 0 {
        return Err(Error::Transport(
            "no management transport is enabled: keep the local channel or configure exactly one of http/https"
                .to_string(),
        ));
    }
    if enabled > 1 {
        return Err(more_than_one());
    }

    match (selection.local, selection.http, selection.https) {
        (Some(LocalForm::Unix(path)), None, None) => {
            #[cfg(unix)]
            {
                let channel = bind::bind_unix(&path)?;
                // The *bound* channel, not the requested path: `state.control_name` is the
                // channel's own identity, which is absolute, because a relative one stops
                // resolving the moment the daemon `chdir("/")`s — which is exactly when
                // `status` and `stop` dial it.
                Ok(Transport::Local { channel })
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport(
                    "unix:// is not a Windows transport".to_string(),
                ))
            }
        }
        (Some(LocalForm::Pipe(name)), None, None) => {
            #[cfg(windows)]
            {
                let channel = bind::bind_pipe(&name)?;
                Ok(Transport::Local { channel })
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport(
                    "pipe:// is not a unix transport".to_string(),
                ))
            }
        }
        (None, Some(addr), None) => {
            // Bound with the runtime-free type and left that way: `serve_tcp` converts inside
            // the runtime, which is the only place `from_std` may be called at all.
            let listener = TcpListener::bind(addr).map_err(Error::Io)?;
            listener.set_nonblocking(true).map_err(Error::Io)?;
            // The bound address, not the requested one: `--port 0` and a wildcard bind both
            // change the answer, and the token rule follows what was actually bound.
            let addr = listener.local_addr().map_err(Error::Io)?;
            Ok(Transport::Tcp { listener, addr })
        }
        (None, None, Some(options)) => {
            #[cfg(feature = "tls")]
            {
                let listener = TcpListener::bind(options.addr).map_err(Error::Io)?;
                listener.set_nonblocking(true).map_err(Error::Io)?;
                let addr = listener.local_addr().map_err(Error::Io)?;
                Ok(Transport::Https {
                    listener,
                    addr,
                    cert: options.cert,
                    key: options.key,
                })
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = options;
                Err(Error::Transport(
                    "https:// needs a build with the `tls` feature".to_string(),
                ))
            }
        }
        // Unreachable by the count above — and written as an `Err` rather than
        // `unreachable!()` because this is library code, where the crate's constraints forbid
        // panics and a `clippy::unreachable` under `-D warnings` would have to be allowed
        // anyway. It is the same refusal the count already made, spelled where the compiler
        // can see it — which is why both hands back come from [`more_than_one`].
        _ => Err(more_than_one()),
    }
}

/// The refusal the count check above and the compiler-visible `_` arm both give: one definition,
/// so the live arm and the dead one cannot drift.
fn more_than_one() -> Error {
    Error::Transport(
        "more than one management transport is enabled: the local channel, http and https are mutually exclusive"
            .to_string(),
    )
}

/// The absolute form of the local channel's socket path: an absolute path is used verbatim, a
/// relative one joins `state_dir` (design decision 24). `state_dir` is absolute — the caller
/// resolved it — so the answer is too, which is what `state.control_name` needs.
///
/// The CLI resolves its configured path through this too, rather than joining it itself: the
/// rule has one owner, so the path recorded in `state.control_name` is the path bound.
pub fn resolve_socket_path(path: &Path, state_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        state_dir.join(path)
    }
}

/// Serves `shared` over a bound TCP listener until `stop` flips, then drains and returns.
///
/// The `std` listener `select` bound becomes tokio's here — inside the runtime, which is the
/// only place `from_std` may be called — and the graceful shutdown is what makes "cease
/// accepting, drain, then exit" one mechanism for the data plane and the control plane alike
/// (decision 12; the ≤ 10 s bound itself is the CLI's, which awaits this future).
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context},
///     state::State,
///     transport::serve_tcp,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// // The shape `select` produces: bound as `std`, non-blocking, converted here.
/// let listener = TcpListener::bind("127.0.0.1:9001")?;
/// listener.set_nonblocking(true)?;
/// let addr = listener.local_addr()?;
/// let state = State::new(addr.to_string(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), stop));
/// serve_tcp(listener, shared, rx).await
/// # }
/// ```
pub async fn serve_tcp(
    listener: TcpListener,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let listener = TokioTcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(listener, router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("tcp channel: {err}")))
}

/// Serves `shared` over TLS with the PEM pair at `cert`/`key`, until `stop` flips.
///
/// The `std` listener `select` bound becomes tokio's **here, inside the runtime**, for the reason
/// [`serve_tcp`] documents: `from_std` panics without a reactor, and `select` ran before one
/// existed. Everything after that conversion is `serve_tcp`'s shape — the same router, the same
/// `until_stopped` — with [`TlsListener`] terminating the handshake on each connection.
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context},
///     state::State,
///     transport::serve_https,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// // The shape `select` produces: bound as `std`, non-blocking, converted inside.
/// let listener = TcpListener::bind("127.0.0.1:9443")?;
/// listener.set_nonblocking(true)?;
/// let addr = listener.local_addr()?;
/// let state = State::new(addr.to_string(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), stop));
/// serve_https(
///     listener,
///     Path::new("/etc/tinio/cert.pem"),
///     Path::new("/etc/tinio/key.pem"),
///     shared,
///     rx,
/// )
/// .await
/// # }
/// ```
#[cfg(feature = "tls")]
pub async fn serve_https(
    listener: TcpListener,
    cert: &Path,
    key: &Path,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let config = tls_config(cert, key)?;
    // The conversion, inside the runtime: `select` bound this before one existed.
    let listener = TokioTcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(TlsListener::new(listener, config), router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("https channel: {err}")))
}

/// The rustls server config, from a PEM chain and key.
///
/// No client authentication: the management plane authenticates with the token header, and a
/// client certificate would be a second scheme nobody configured.
#[cfg(feature = "tls")]
fn tls_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>, Error> {
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(
        fs::File::open(cert).map_err(Error::Io)?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|err| Error::Transport(format!("{}: {err}", cert.display())))?;
    if certs.is_empty() {
        return Err(Error::Transport(format!(
            "{}: no certificate",
            cert.display()
        )));
    }
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(
        fs::File::open(key).map_err(Error::Io)?,
    ))
    .map_err(|err| Error::Transport(format!("{}: {err}", key.display())))?
    .ok_or_else(|| Error::Transport(format!("{}: no private key", key.display())))?;

    // No process-level provider installation needed, and two reasons — the only two that hold.
    // **One provider is compiled in**: the workspace pin (`tokio-rustls` with
    // `default-features = false` and `ring`, `docs/cargo.md`) admits no other, so `builder()`
    // cannot hit the ambiguous-provider panic that would otherwise require an explicit
    // `CryptoProvider::install_default`. **No CMake or NASM is required**: `ring` is not a
    // pure-Rust provider and does build C — it lists `cc` and pulls `windows-sys 0.52.0` into
    // `Cargo.lock` — but that is a plain C compile, where `aws-lc-rs` would additionally demand
    // both CMake and NASM. Do not restate this as "no C toolchain".
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| Error::Transport(format!("TLS configuration: {err}")))?;
    Ok(Arc::new(config))
}

/// An axum [`Listener`] that terminates TLS on each accepted connection,
/// completing the handshake before it hands the connection over.
///
/// **The handshake runs inside `accept`, and that is the whole reason `HANDSHAKE_TIMEOUT`
/// exists.** `TlsAcceptor::accept` has no timeout of its own, and axum spawns its per-connection
/// task only *after* `Listener::accept` returns (`axum-0.8.9/src/serve/mod.rs:189-191`), so
/// without the timeout a client that connected and then said nothing would park the accept loop —
/// and `/status`, `/stop` and the readiness probe behind it — for as long as it stayed connected,
/// unauthenticated, at the price of one TCP connection.
///
/// **No `Debug` derive, deliberately**: [`tokio_rustls::TlsAcceptor`] implements only `Clone`
/// (`tokio-rustls-0.26.5/src/server.rs:18-21`), so deriving `Debug` here does not compile, and
/// nothing needs it — `axum::serve` awaits a listener without printing it.
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, sync::Arc};
///
/// use tinio_api::transport::TlsListener;
/// use tokio_rustls::rustls::ServerConfig;
///
/// # fn config() -> Arc<ServerConfig> { unimplemented!() }
/// # fn router() -> axum::Router { unimplemented!() }
/// # async fn serve() -> Result<(), Box<dyn std::error::Error>> {
/// let listener = TcpListener::bind("127.0.0.1:9443")?;
/// listener.set_nonblocking(true)?;
/// // `from_std` needs the reactor, so the conversion belongs inside the runtime; `serve_https`
/// // is where this crate does it.
/// let listener = tokio::net::TcpListener::from_std(listener)?;
/// // `axum::serve` drives it like any other listener: every connection it accepts has already
/// // completed its handshake, and every one it could not complete was dropped on its own.
/// axum::serve(TlsListener::new(listener, config()), router()).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tls")]
pub struct TlsListener {
    inner: TokioTcpListener,
    acceptor: TlsAcceptor,
}

/// How long a connection may take to complete its TLS handshake before it is dropped.
///
/// Five seconds is orders of magnitude more than a real handshake needs on a management port,
/// and the bound is what keeps a silent or stalled client from holding the accept loop
/// (`TlsListener`'s doc states the mechanism). It is a liveness floor, not a performance target:
/// nothing in this crate measures a handshake.
#[cfg(feature = "tls")]
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "tls")]
impl TlsListener {
    /// Wraps a bound TCP listener, terminating TLS with `config`.
    ///
    /// The listener arrives as tokio's own type, never `std`'s: `from_std` needs the reactor, so
    /// the conversion belongs at the call site that has one — which is
    /// [`serve_https`], not here.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{net::TcpListener, sync::Arc};
    ///
    /// use axum::serve::Listener;
    /// use tinio_api::transport::TlsListener;
    /// use tokio_rustls::rustls::ServerConfig;
    ///
    /// # fn config() -> Arc<ServerConfig> { unimplemented!() }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let listener = TcpListener::bind("127.0.0.1:9443")?;
    /// listener.set_nonblocking(true)?;
    /// let listener = tokio::net::TcpListener::from_std(listener)?;
    /// let listener = TlsListener::new(listener, config());
    /// // It is still a listener: the bound address answers as it did before the wrap.
    /// assert_eq!(listener.local_addr()?.port(), 9443);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(listener: TokioTcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            inner: listener,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

#[cfg(feature = "tls")]
impl Listener for TlsListener {
    type Addr = SocketAddr;
    type Io = TlsStream<TcpStream>;

    /// The trait's method has **no** `Result` (`axum-0.8.9/src/serve/listener.rs:9-24`: "if the
    /// underlying accept call can return an error, this function must take care of logging and
    /// retrying"), so both failures below are absorbed here rather than surfaced: an `accept`
    /// failure retries with axum's own back-off — silence for a client hang-up, one `error!` and
    /// a one-second sleep for anything else, on every occurrence, which is what keeps a persistent
    /// `EMFILE` a slow loop instead of a spin (`listener.rs:140-163`) — and a handshake that
    /// fails or times out drops that one client, never the loop.
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    // The back-off goes with the log, exactly as axum pairs them: a connection
                    // error is routine and retried at once, anything else is logged and slept on
                    // so a listener out of handles or out of addresses cannot spin.
                    if !is_connection_error(&err) {
                        tracing::error!(error = %err, "the TLS listener's accept failed");
                        sleep(Duration::from_secs(1)).await;
                    }
                    continue;
                }
            };
            match timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(stream)).await {
                Ok(Ok(stream)) => return (stream, addr),
                Ok(Err(err)) => {
                    tracing::debug!(%addr, error = %err, "TLS handshake failed");
                }
                Err(_) => {
                    // The socket goes with the elapsed future: the client that never spoke
                    // releases the loop instead of holding it.
                    tracing::debug!(%addr, "TLS handshake timed out");
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Whether an accept error is a client hanging up rather than a listener problem: those are
/// routine, and logging each one would be noise, not information (`axum`'s own three kinds).
#[cfg(feature = "tls")]
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

impl Transport {
    /// What `state.control_name` records: the bound socket path, the pipe name, or the
    /// address. A client reads this value out of `state` and never re-derives it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_api::transport::{Selection, select};
    ///
    /// let transport = select(Selection {
    ///     http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///     ..Selection::default()
    /// })
    /// .expect("bind");
    /// assert_eq!(transport.identity(), "127.0.0.1:9001");
    /// ```
    pub fn identity(&self) -> String {
        match self {
            Self::Local { channel } => channel.identity(),
            Self::Tcp { addr, .. } => addr.to_string(),
            #[cfg(feature = "tls")]
            Self::Https { addr, .. } => addr.to_string(),
        }
    }

    /// The token rule this transport carries (`management-api.md`'s matrix).
    ///
    /// The rule is derived here rather than stored: the local channel is always [`Auth::Local`],
    /// and a TCP transport is [`Auth::for_bind`] of the address it **bound** — so the
    /// bind-address rule has one home, and a transport cannot carry a rule that disagrees with
    /// its own address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_api::{
    ///     router::Auth,
    ///     transport::{Selection, select},
    /// };
    ///
    /// // The rule follows the *bind* address: a reachable one is the strict rule.
    /// let transport = select(Selection {
    ///     http: Some("0.0.0.0:9001".parse().expect("addr")),
    ///     ..Selection::default()
    /// })
    /// .expect("bind");
    /// assert_eq!(transport.auth(), Auth::Tcp);
    /// ```
    pub fn auth(&self) -> Auth {
        match self {
            Self::Local { .. } => Auth::Local,
            Self::Tcp { addr, .. } => Auth::for_bind(*addr),
            #[cfg(feature = "tls")]
            Self::Https { addr, .. } => Auth::for_bind(*addr),
        }
    }
}

/// What the CLI hands the plane: the transport input, where the plane's files go, and the
/// shutdown channel it sends on.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::{Options, Plane, transport::Selection};
/// use tokio::sync::watch;
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// let (stop, _rx) = watch::channel(false);
/// let options = Options {
///     selection: Selection {
///         http: Some("127.0.0.1:9001".parse().expect("addr")),
///         ..Selection::default()
///     },
///     state_dir: PathBuf::from("/run/tinio"),
///     root: PathBuf::from("/srv/data"),
///     host: "127.0.0.1".to_string(),
///     stop,
///     gauges: None,
///     token: None,
///     #[cfg(feature = "lifecycle")]
///     lifecycle: None,
/// };
/// let plane = Plane::bind(options)?;
/// plane.publish(9000)?;
/// # Ok(())
/// # }
/// ```
#[derive(derive_more::Debug)]
pub struct Options {
    /// The three-choose-one input, as `[api]` and the `--api*` flags resolved it.
    pub selection: Selection,
    /// The resolved state directory (design decision 6) — the plane's only filesystem anchor,
    /// and absolute: a **relative** `[api.unix] path` in `selection` joins this directory
    /// before anything binds ([`Plane::bind`]), so what the bind creates and what `state`
    /// records are the same absolute path.
    pub state_dir: PathBuf,
    /// The served root, canonical: `state.root`, and the input the socket name derives from.
    pub root: PathBuf,
    /// The data plane's host, as `/status` reports it.
    pub host: String,
    /// The shutdown channel `/stop` sends on — the same channel the CLI's signal handlers
    /// flip and `DataPlane::serve` reads (decision 12).
    pub stop: watch::Sender<bool>,
    /// The scrape-time gauge source the plane's `/metrics` serves, or `None` for a plane
    /// without one (the route then answers the contract's `404`, `management-api.md:23`). It is
    /// an input rather than something the plane builds because only the caller can: the
    /// registry side is `tinio-server`'s, and `tinio-api` must not depend on that crate (design
    /// decision 23 — cargo refuses the cycle). The **reader task** that runs the source's
    /// expensive half starts with the plane, in [`Plane::serve`].
    pub gauges: Option<Arc<Gauges>>,
    /// The token this plane serves with, when the caller supplies one instead of letting the
    /// plane generate it.
    ///
    /// **`Some` is the launcher's token, and the launcher is also the process that wrote
    /// `state`** — which is why the caller has it to pass: a confined child on Windows cannot
    /// create that file, so the unrestricted launcher does, generating the token with the file.
    /// This plane then verifies `/status` and `/stop` against [`State::with_token`]'s value
    /// rather than a fresh one, so the token a client reads out of that file is the token that
    /// opens the channel (see the field's doc there for what a child that minted its own would
    /// answer). `None` is every plane that owns its `state` file, which is every unconfined run
    /// and every in-place platform — and it is what [`Plane::publish`] is for.
    pub token: Option<String>,
    /// The lifecycle admin the plane's three `/buckets/{bucket}/lifecycle` routes are served
    /// from, or `None` for a plane that does not serve them (LC14). **An input for the same
    /// reason [`Options::gauges`] is**: only the caller holds the storage those routes read, and
    /// `tinio-api` must not depend on the crate that owns it (cargo's cycle rule). A build
    /// without the `lifecycle` feature has no field and no trait (LC15), and a run whose `[s3]
    /// lifecycle` is off passes `None` (LC23) — both causes answer the three routes' `404`, so
    /// nothing can replace a stored configuration the plane cannot serve.
    #[cfg(feature = "lifecycle")]
    #[debug("<injected>")]
    pub lifecycle: Option<Arc<dyn LifecycleAdmin>>,
}

/// The `state` this plane serves under: the control channel's identity, the served root, and
/// **the caller's token when it supplied one** ([`Options::token`]) — a fresh one otherwise.
///
/// **One rule, two binds.** `Plane::bind` and `Plane::adopt` both build the state, and the
/// choice between "the launcher's token" and "one of my own" is the same choice in both — so it
/// is stated once here rather than twice, where a later edit to one of them could silently
/// leave the other minting a token for a file it does not own. The port is `None` at this point
/// in both: it is the data plane's, and `publish` is where it arrives.
fn state_for(
    token: Option<String>,
    control_name: impl Into<String>,
    root: &Path,
) -> Result<State, Error> {
    match token {
        Some(token) => State::with_token(control_name, None, root, token),
        None => State::new(control_name, None, root),
    }
}

/// The socket file the bind created, as the deletion path must still find it: the path, and the
/// identity it had then.
///
/// A successor's `bind_unix` reclaims a stale path by unlinking it and binding again
/// (`crate::bind::reclaim_stale` is what makes that safe to do behind a crashed instance), and
/// what it leaves at the same name is a **different file**. Removing by name alone would
/// therefore unlink a live server's socket — and, with the path free again, let a third server
/// bind it, which is single-instance enforcement gone as well as discovery. The recorded identity
/// is what tells the two apart. Windows carries no socket file at all (its `Channel` owns a
/// name, not a path), so there the path is all there is and nothing is ever constructed in
/// practice: [`Channel::socket_path`] answers `None` on that platform.
#[derive(Debug)]
struct SocketFile {
    /// The path the bind created.
    path: PathBuf,
    /// `(dev, ino)` at the bind (`std::os::unix::fs::MetadataExt`), which a replacement changes.
    #[cfg(unix)]
    id: (u64, u64),
}

impl SocketFile {
    /// Records the file at `path` as it is now — called immediately after the bind, so what is
    /// recorded is what the bind created.
    fn at(path: PathBuf) -> Result<Self, Error> {
        #[cfg(unix)]
        {
            let metadata = fs::metadata(&path).map_err(Error::Io)?;
            Ok(Self {
                path,
                id: (metadata.dev(), metadata.ino()),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self { path })
        }
    }

    /// Whether the file at `path` is still the one recorded above: on unix, the same `(dev, ino)`.
    /// An unreadable file is **not** ours — an unproven claim is not a licence to delete — and a
    /// missing one has nothing left to remove, which the caller's `NotFound` arm also covers.
    fn still_bound(&self) -> bool {
        #[cfg(unix)]
        {
            match fs::metadata(&self.path) {
                Ok(metadata) => (metadata.dev(), metadata.ino()) == self.id,
                Err(_) => false,
            }
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}

/// The management plane: the bound transport, the router it serves, and the two files it owns.
///
/// **One owner for both artifacts** (decision 12): the plane creates the socket at
/// [`Plane::bind`] and owns the `state` file, so it is the plane that writes and removes them —
/// through [`Plane::publish`] (the readiness point) and [`Plane::shutdown`]. The CLI never
/// joins those file names itself, and [`Drop`] is a best-effort backstop for the paths that do
/// not go through `shutdown`.
///
/// **The client never deletes, and the reason is a race.** A `status` or `stop` run removes
/// nothing, because a client that unlinks a socket or a `state` file cannot know whether the
/// server behind it is still live. The one exception is `doctor --fix` (plan C), and it is not
/// a client: it reclaims a stale socket through `crate::bind::reclaim_stale` (the dry-run arm
/// is what its report is built from) and a stale `state` through [`State::remove`], and only
/// after a probe has proved no server answers for that root — the proof is what the rule
/// exists to require, not a second way around it.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::{Options, Plane, transport::Selection};
/// use tokio::sync::watch;
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // The CLI's sequence, in order: bind (which is the single-instance lock) → publish
/// // (the readiness point) → serve → shutdown (the plane's own deletions).
/// let (stop, rx) = watch::channel(false);
/// let plane = Plane::bind(Options {
///     selection: Selection {
///         http: Some("127.0.0.1:9001".parse().expect("addr")),
///         ..Selection::default()
///     },
///     state_dir: PathBuf::from("/run/tinio"),
///     root: PathBuf::from("/srv/data"),
///     host: "127.0.0.1".to_string(),
///     stop,
///     gauges: None,
///     token: None,
///     #[cfg(feature = "lifecycle")]
///     lifecycle: None,
/// })?;
/// plane.publish(9000)?;
/// plane.serve(rx).await?;
/// plane.shutdown().await
/// # }
/// ```
#[derive(Debug)]
pub struct Plane {
    /// Taken (once) by `serve`: a listener cannot be cloned, so serving consumes it.
    transport: Mutex<Option<Transport>>,
    shared: Shared,
    state_dir: PathBuf,
    /// The socket file the bind created, with the identity it had then: the deletion path
    /// removes it only while it is still that file (see `Plane::remove_owned`). Windows never
    /// records one — the Windows `Channel` has no path — so it is always `None` there.
    socket: Option<SocketFile>,
}

impl Plane {
    /// [`Plane::bind`]'s mirror for a process a launcher **confined**: the control channel
    /// is already created, and this process serves on the instances it inherited.
    ///
    /// **Why adoption rather than a second bind.** The launcher binds the channel because
    /// only it can: the pipe carries Windows' own default named-pipe descriptor, which names the
    /// launcher's user and not the package SID — and inside a container this process's identity
    /// is the package SID, so its own `CreateNamedPipeW` is refused `ERROR_ACCESS_DENIED`, and a
    /// child that tried would fail its startup with a message about a second server. The launcher
    /// instead creates the pool, keeps the single-instance claim, and passes every handle the way
    /// it passes the data plane's listener (sandbox design decision 16, extended to the control
    /// channel); nothing about the descriptor, the identity of the claimant, or the refusal a
    /// genuinely concurrent start gets changes.
    ///
    /// **The whole pool and not one handle**: `raws` is what the launcher handed over, and
    /// each element becomes an instance of the channel `serve` rotates (`crate::bind::INSTANCES`
    /// is the count both sides of that handoff use). A child given one instance is back to the
    /// defect this exists to remove — the name answers once and then goes deaf — so a handoff that
    /// carries too few is refused here rather than served as a channel that would stop answering.
    ///
    /// **The name is resolved here and not derived a second time.** `options.selection` is
    /// the same `Selection` the launcher bound — both processes resolve it through the
    /// CLI's one `api_options` rule against the same root — and `crate::bind::pipe_name`
    /// normalises it exactly as [`crate::bind::bind_pipe`] did,
    /// so `state.control_name` records the name the pipe actually carries. The local form
    /// must be a [`LocalForm::Pipe`]: this platform's channel is a pipe, and a selection
    /// that names none is the launcher's own contradiction rather than a run this process
    /// may serve.
    ///
    /// The rest is [`Plane::bind`]'s, deliberately in the same order: the identity is
    /// recorded from the channel, the `state` file is built around it, and the router is
    /// built with the token rule for the local channel. No socket file is recorded —
    /// [`Channel::socket_path`] answers `None` here — which is why [`Plane::shutdown`]
    /// removes only the `state` file on this platform.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] for a selection with no pipe channel in it or a handoff
    /// shorter than `crate::bind::INSTANCES`, and [`crate::Error::Io`] when an inherited handle
    /// is not one — a null value, which no real handoff produces. An unreadable served root is
    /// [`crate::Error::State`], as in the bind.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use std::os::windows::io::RawHandle;
    ///
    /// use tinio_api::{
    ///     Options, Plane,
    ///     transport::{LocalForm, Selection},
    /// };
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // The handles a launcher put on the child's argv — the values `Adopted::controls()`
    /// // carried, one instance each. `crate::bind::INSTANCES` of them: `adopt` refuses fewer,
    /// // because one instance makes the name deaf after its first answer.
    /// # let inherited: [RawHandle; 2] = [0x1000usize as _, 0x1004usize as _];
    /// let plane = Plane::adopt(
    ///     Options {
    ///         selection: Selection {
    ///             local: Some(LocalForm::Pipe(r"\\.\pipe\tinio-mine".to_string())),
    ///             ..Selection::default()
    ///         },
    ///         state_dir: "/run/tinio".into(),
    ///         root: "/srv/data".into(),
    ///         host: "127.0.0.1".to_string(),
    ///         stop,
    ///         gauges: None,
    ///         // The launcher's token, when this plane is the child it confined.
    ///         token: None,
    ///         #[cfg(feature = "lifecycle")]
    ///         lifecycle: None,
    ///     },
    ///     &inherited,
    /// )?;
    /// // **No `publish` here, and that is the ruling rather than an omission**: this plane's
    /// // `state` file is the launcher's — it wrote it, with the token above — and this process
    /// // does not write it: the plane serves, and the launcher publishes.
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    #[cfg(windows)]
    pub fn adopt(options: Options, raws: &[RawHandle]) -> Result<Self, Error> {
        // The local form, resolved for its **name** alone — the handles are the objects, and
        // this name is what the state file records. Same rule, same definer as the bind's.
        let Some(LocalForm::Pipe(name)) = options.selection.local.as_ref() else {
            return Err(Error::Transport(
                "a confined child must be handed a named-pipe control channel: this selection \
                 names none, so there is nothing to adopt"
                    .to_string(),
            ));
        };
        // **Fewer instances than the channel needs is a refused handoff, not a channel to serve.**
        // A client only ever reaches a *listening* instance, and this process cannot create the
        // next one — `CreateNamedPipeW` on the launcher's pipe is refused inside the container,
        // whose descriptor names the launcher's user rather than the package SID — so a pool of one
        // answers its first client and is then connected for as
        // long as it serves: the name goes deaf, which is exactly the defect the pool exists to
        // remove. `bind::INSTANCES` is the floor and the same constant the bind creates and the
        // launcher hands over, so a future change to the pool size moves this check with it
        // instead of leaving a silent downgrade behind. More than the floor is fine: extra
        // instances only mean more listeners.
        if raws.len() < bind::INSTANCES {
            return Err(Error::Transport(format!(
                "a confined child must be handed the control channel's whole pool: {} instance(s) \
                 were handed over and {} are needed — one instance makes the name deaf after its \
                 first answer, and this process cannot create the next one",
                raws.len(),
                bind::INSTANCES
            )));
        }
        let name = bind::pipe_name(name);
        // The raw conversion, and it is `pipe`'s: this crate's designated `unsafe` site, where
        // the launcher's handoff is the safety argument. One instance per inherited handle.
        let mut handles = Vec::with_capacity(raws.len());
        for raw in raws {
            handles.push(pipe::adopted_pipe(*raw)?);
        }
        let transport = Transport::Local {
            channel: Channel { name, handles },
        };
        Self::assemble(options, transport)
    }

    /// [`Plane::bind`]'s mirror for a process a launcher **confined**: the local control channel
    /// is already bound, and this process serves on the socket it inherited.
    ///
    /// **Why adoption rather than a second bind.** The macOS profile grants the confined child no
    /// `network-bind` at all, so a child that called [`crate::bind::bind_unix`] would die with
    /// `could not bind … Operation not permitted` — and a rule that permitted the bind would be a
    /// grant on every socket rather than a filter on the one the child wants. The launcher is
    /// unconfined, so *it* binds the channel, and the child serves on the descriptor it inherits;
    /// the profile therefore carries no network rule, exactly as it carries none for the data
    /// plane, whose listener travels the same way (sandbox design decision 16, extended to the
    /// control channel).
    ///
    /// **The single-instance claim is the launcher's, and it is neither weakened nor duplicated.**
    /// The launcher's [`crate::bind::bind_unix`] is the bind that reclaimed a stale path and
    /// claimed the socket file, so a second server on the same root is refused there — before any
    /// child exists. This function binds nothing: it records the channel it was handed.
    ///
    /// **The identity is resolved here through the one definer, and not re-derived from the
    /// descriptor.** `options.selection` is the same `Selection` the launcher bound — both
    /// processes resolve it through the CLI's one `api_options` rule against the same root — and
    /// [`resolve_socket_path`] turns its `[api.unix] path` into the absolute path
    /// [`Plane::bind`] would have bound, so `state.control_name` records the path the launcher's
    /// [`crate::bind::Channel::identity`] reported and the child actually serves on. The local
    /// form must be a [`LocalForm::Unix`]: this platform's channel is a socket file, and a
    /// selection that names none is the launcher's own contradiction rather than a run this
    /// process may serve.
    ///
    /// The rest is [`Plane::bind`]'s, deliberately in the same order: the identity is recorded
    /// from the channel, the `state` file is built around it, and the router is built with the
    /// token rule for the local channel. The socket file is recorded too — the launcher created
    /// it, and the child is the process that serves on it and so the one that removes it on the
    /// way out, under the same identity check [`Plane::bind`]'s deletion uses.
    ///
    /// **The listener arrives as an object and not as an integer.** Turning an inherited
    /// descriptor number back into a socket is `unsafe`, and this crate carries no site for it;
    /// the platform crate that rebuilds it (the macOS backend's
    /// `tinio_sandbox_macos::launch::control`, one of that crate's **three** enumerated
    /// `#[expect(unsafe_code)]` sites — the `inherited_fd` rebuild, the libSystem FFI module the
    /// in-process mechanism applies through, and that mechanism's validator's `fork`) is what
    /// hands the object over.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] for a selection with no unix channel in it, and
    /// [`crate::Error::State`] for an unreadable served root — the bind's own failures, minus
    /// everything about binding.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # {
    /// use std::{os::unix::net::UnixListener, path::PathBuf};
    ///
    /// use tinio_api::{
    ///     Options, Plane,
    ///     transport::{LocalForm, Selection},
    /// };
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // The launcher's half: it bound the socket and cleared `FD_CLOEXEC` on it. What this
    /// // process holds is the descriptor that crossed the exec, rebuilt into a listener by the
    /// // platform crate.
    /// let listener = UnixListener::bind("/tmp/tinio-0123456789abcdef.sock")?;
    /// let plane = Plane::adopt(
    ///     Options {
    ///         selection: Selection {
    ///             local: Some(LocalForm::Unix(PathBuf::from(
    ///                 "/tmp/tinio-0123456789abcdef.sock",
    ///             ))),
    ///             ..Selection::default()
    ///         },
    ///         state_dir: PathBuf::from("/tmp"),
    ///         root: PathBuf::from("/srv/data"),
    ///         host: "127.0.0.1".to_string(),
    ///         stop,
    ///         gauges: None,
    ///         // The launcher's token, when this plane is the child it confined.
    ///         token: None,
    ///         #[cfg(feature = "lifecycle")]
    ///         lifecycle: None,
    ///     },
    ///     listener,
    /// )?;
    /// // **No `publish` here, and that is the ruling rather than an omission**: this plane's
    /// // `state` file is the launcher's — it wrote it, with the token above — so the plane
    /// // serves, and the launcher publishes.
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    #[cfg(unix)]
    pub fn adopt(options: Options, listener: UnixListener) -> Result<Self, Error> {
        // The local form, resolved for its **path** alone — the socket is the object, and this
        // path is what the state file records. Same rule, same definer as the bind's.
        let Some(LocalForm::Unix(path)) = options.selection.local.as_ref() else {
            return Err(Error::Transport(
                "a confined child must be handed a unix control channel: this selection names \
                 none, so there is nothing to adopt"
                    .to_string(),
            ));
        };
        let channel = Channel {
            listener,
            path: resolve_socket_path(path, &options.state_dir),
        };
        let transport = Transport::Local { channel };
        Self::assemble(options, transport)
    }

    /// Bind a new control channel, or adopt the one a launcher already handed this process.
    ///
    /// An empty `controls` list is every run that owns the single-instance claim itself: unconfined,
    /// in-place, or a selection this process binds. A non-empty list is the inherited pool.
    /// Windows adopts those values as pipe handles. macOS adopts `socket`, the listener the sandbox
    /// rebuilt from the inherited descriptor — this crate does not turn a descriptor number back
    /// into a socket. Any other target refuses a non-empty list: an in-place process was not handed
    /// a channel.
    ///
    /// **`socket` is a parameter on macOS alone, and it is `#[cfg]`'d rather than passed by every
    /// caller.** It used to be an `Option<InheritedSocket>` everywhere — a placeholder struct with
    /// an `absent()` constructor on the targets that inherit no unix control socket — which made
    /// four places agree about a value two of the three platforms never read: the two type
    /// definitions, the caller's conversion into them, and the `let _ = socket;` this body used to
    /// write. The axis is the sandbox facade's own (`tinio_sandbox::platform_facts` reports a
    /// control socket on macOS alone, and `tinio_sandbox::adopted_control` succeeds for that target
    /// only), so the parameter carries the same `#[cfg]`. Windows never names it — its control
    /// channel is a named pipe whose handles travel as `controls` values — and neither does Linux,
    /// whose confinement is in place and whose serving process is the one that bound. **The
    /// signature therefore differs by target**, which is the honest spelling of a handoff only one
    /// mechanism performs.
    ///
    /// # Errors
    ///
    /// The bind's errors, the adopt's errors, a macOS handoff with no socket, or a non-empty list
    /// on a target that does not launch a child.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// let options = Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    ///     #[cfg(feature = "lifecycle")]
    ///     lifecycle: None,
    /// };
    /// // An empty list binds on every target, which is the arm this example can spell once — and
    /// // the argument macOS alone carries is spelled on the arm that has it.
    /// # #[cfg(target_os = "macos")]
    /// let plane = Plane::open(options, &[], None)?;
    /// # #[cfg(not(target_os = "macos"))]
    /// let plane = Plane::open(options, &[])?;
    /// let _ = plane;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(
        options: Options,
        controls: &[i64],
        #[cfg(target_os = "macos")] socket: Option<UnixListener>,
    ) -> Result<Self, Error> {
        if controls.is_empty() {
            return Self::bind(options);
        }
        #[cfg(windows)]
        {
            let raws: Vec<RawHandle> = controls
                .iter()
                .copied()
                .map(|raw| raw as usize as RawHandle)
                .collect();
            Self::adopt(options, &raws)
        }
        #[cfg(target_os = "macos")]
        {
            let Some(socket) = socket else {
                return Err(Error::Transport(
                    "a confined child must be handed a unix control channel: the inherited \
                     descriptors name one, and this process was not given the socket"
                        .to_string(),
                ));
            };
            Self::adopt(options, socket)
        }
        #[cfg(not(any(windows, target_os = "macos")))]
        {
            Err(Error::Transport(
                "this platform confines in place and does not adopt an inherited control channel"
                    .to_string(),
            ))
        }
    }

    /// Applies the three-choose-one rule and binds the winner — which **is** the
    /// single-instance lock (decision 12) — then builds the router around it.
    ///
    /// The local form is resolved to an **absolute** path *before* the bind (design decision 24,
    /// and the rule [`crate::state::socket_path`]'s doc states): an absolute `[api.unix] path`
    /// is used verbatim, a relative one joins `state_dir`, and the path the bind creates is
    /// therefore the identity `state.control_name` records. A relative name would stop
    /// resolving the moment the daemon `chdir("/")`s — which is exactly when `status` and
    /// `stop` need to dial it — so this is a correctness step, not a convenience.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // No runtime in sight: the TCP arm binds with `std` and `serve` converts it.
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    ///     #[cfg(feature = "lifecycle")]
    ///     lifecycle: None,
    /// })?;
    /// plane.publish(9000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(mut options: Options) -> Result<Self, Error> {
        // **The selection leaves `options` rather than being cloned out of it.** `select` consumes
        // the selection while [`Plane::assemble`] takes the whole `Options`, and the two needs used
        // to be met with a clone — of three `Option`s, which is why it was affordable, and of
        // nothing the assemble reads, which is why the take is the same shape with no copy. What
        // stays behind is `Selection::default()`, and nothing downstream looks at it: the assemble
        // reads the token, the root, the host, the stop channel, the gauges and the state dir, and
        // the transport `select` just built is its answer to the selection.
        let mut selection = mem::take(&mut options.selection);
        if let Some(LocalForm::Unix(path)) = &mut selection.local {
            *path = resolve_socket_path(path, &options.state_dir);
        }
        let transport = select(selection)?;
        Self::assemble(options, transport)
    }

    /// The tail [`Plane::bind`] and both [`Plane::adopt`]s share: the `state` file built from the
    /// transport's identity, the socket file that identity records when the transport has a path,
    /// and the router around both.
    ///
    /// **One rule, three constructions**, and the order is the bind's: the identity is recorded as
    /// the socket file is built because that is the only moment it is certainly this plane's file
    /// (`Plane::remove_owned` compares against it later), and the `state` file is built around
    /// the identity before the router exists. Both adopts hand over a channel the **launcher**
    /// created, so what they record is the launcher's file as it stands when they read it; that is
    /// the same rule, not a second one.
    ///
    /// **The `_ => None` arm is reachable, and [`Plane::bind`] is what reaches it.** [`select`]
    /// answers [`Transport::Tcp`] (and `Https`, under `tls`) for an http/https selection, and
    /// neither carries a socket path; only [`Transport::Local`] has one to record. Both adopts
    /// construct `Local` and so never arrive there — but the arm is the bind's, not theirs, so it
    /// stays in the one shape all three go through rather than being narrowed to the adopts.
    ///
    /// **What is recorded is not checked against `options.selection`.** The adopts trust the
    /// object they were handed to be the channel at the path that selection names — meeting by
    /// construction, which is the launcher's and the platform crate's to guarantee, as
    /// [`Plane::adopt`] states; nothing here re-derives or compares the two.
    ///
    /// The caller's source travels in with the context and nowhere else, so the route's default
    /// state stays `None` (`Context::new`): a plane built without one has no `/metrics`, and the
    /// API tests can assert both halves of that.
    fn assemble(options: Options, transport: Transport) -> Result<Self, Error> {
        let state = state_for(options.token.clone(), transport.identity(), &options.root)?;
        let socket = match &transport {
            Transport::Local { channel } => {
                channel.socket_path().map(SocketFile::at).transpose()?
            }
            _ => None,
        };
        let context = Context::new(state, options.host, transport.auth(), options.stop)
            .with_gauges(options.gauges);
        #[cfg(feature = "lifecycle")]
        let context = context.with_lifecycle(options.lifecycle);
        let shared = Arc::new(context);
        Ok(Self {
            transport: Mutex::new(Some(transport)),
            shared,
            state_dir: options.state_dir,
            socket,
        })
    }

    /// Serves until the shutdown channel flips — the plane's own `/stop` sends on that same
    /// channel — then stops accepting, drains the connections already accepted, and returns.
    /// The transport is taken out of the plane here, which makes this a one-shot call; the
    /// drain is `axum::serve`'s `with_graceful_shutdown` (and, on Windows, the pipe loop's
    /// `JoinSet`), and the ≤ 10 s bound on it is the CLI's — this future is what the CLI awaits
    /// with that bound (decision 12), so the plane never invents a second timeout.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # async fn run() -> Result<(), tinio_api::Error> {
    /// let (stop, rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    ///     #[cfg(feature = "lifecycle")]
    ///     lifecycle: None,
    /// })?;
    /// // `/stop` sends on the very channel this awaits: the plane can end its own serve,
    /// // and the CLI's signal handlers flip the same one.
    /// plane.serve(rx).await
    /// # }
    /// ```
    pub async fn serve(&self, stop: watch::Receiver<bool>) -> Result<(), Error> {
        let transport = self
            .release_transport()
            .ok_or_else(|| Error::Internal("the plane is already serving".to_string()))?;

        // The metrics reader starts here, the one place that has both the `Arc<Gauges>` and the
        // runtime: it watches this same channel, so it exits with the plane rather than being
        // aborted. Holding the handle is what makes that visible in this frame, and nothing joins
        // it — a dropped `JoinHandle` only **detaches** its task, so `let _ = ...` here would
        // leave the very same loop running; what ends it is the channel, not this function's
        // return. A plane with `gauges: None` starts nothing.
        let _reader = self
            .shared
            .gauges()
            .map(|gauges| gauges.spawn_reader(stop.clone()));
        serve_transport(transport, self.shared.clone(), stop).await
    }

    /// Writes `state` with the data plane's port — the readiness point (decision 12), after
    /// both planes are bound. `/status` reports the same port from here on, and a second call
    /// changes neither it nor the file: published once, the first port is the one both carry.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    ///     #[cfg(feature = "lifecycle")]
    ///     lifecycle: None,
    /// })?;
    /// // The data plane's port, and the moment `status` can find this server.
    /// plane.publish(9000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&self, port: u16) -> Result<(), Error> {
        // Published once: a second call keeps the first port, which is what the file holds and
        // what `/status` reports — so the two cannot disagree.
        if self.shared.port.get().is_some() {
            return Ok(());
        }
        let mut state = self.shared.state.clone();
        state.port = Some(port);
        state.write(&self.state_dir)?;
        let _ = self.shared.port.set(port);
        Ok(())
    }

    /// The plane's last act, and the plane's only deletion site — `doctor --fix` reclaims the
    /// same two files, but only after proving no server answers, through `reclaim_stale` and
    /// `State::remove`. The serves have already ended on the shared channel (the CLI's bounded
    /// `≤ 10 s` wait is on their futures). Async so the CLI's shutdown path is one `await` in
    /// the sequence that also awaits the data plane's drain.
    ///
    /// **It removes only what this plane still owns** — the socket file when it is still the one
    /// the bind created, and `state` when it still names this process. The gate is released
    /// before this runs, so a successor can already hold the path (see `Plane::remove_owned`);
    /// deleting by name alone would strip that live server of both artifacts.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # async fn run() -> Result<(), tinio_api::Error> {
    /// let (stop, rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    ///     #[cfg(feature = "lifecycle")]
    ///     lifecycle: None,
    /// })?;
    /// plane.publish(9000)?;
    /// plane.serve(rx).await?;
    /// // The socket and `state` are the plane's to remove, after the serves have ended.
    /// plane.shutdown().await
    /// # }
    /// ```
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.remove_owned()
    }

    /// Removes the two artifacts this plane still owns — the last act of [`Plane::shutdown`],
    /// and of [`Drop`] when a caller never reaches it. One implementation, so the two cannot
    /// drift; only their error handling differs.
    ///
    /// **Ownership, not liveness.** The plane's bind *is* the single-instance gate, and the gate
    /// is released the moment `serve` returns — while the CLI still has the data plane's drain to
    /// await before it calls `shutdown`. A successor that passes the gate inside that window (the
    /// unix bind reclaims whatever stale path it finds) has by then published its own `state` and
    /// bound its own socket, so a deletion by name alone would delete **the successor's** live
    /// socket and `state` — stripping a running server of discovery and, with the path free
    /// again, letting a third one in. Each artifact is therefore checked against what this plane
    /// created: the socket by file identity (`(dev, ino)`, which a replacement changes) and
    /// `state` by the pid it names (`State::new` writes this process's; a successor's names its
    /// own).
    ///
    /// Anything not demonstrably ours is left alone: a `state` file that will not parse, cannot
    /// be read, or names another process is not this plane's to remove.
    fn remove_owned(&self) -> Result<(), Error> {
        // One condition, two halves of "ours": the plane bound a socket file at all, and the file
        // at that path is still the one it bound.
        if let Some(socket) = &self.socket
            && socket.still_bound()
        {
            remove_if_present(&socket.path)?;
        }
        match State::read(&self.state_dir) {
            Ok(Some(state)) if state.pid == process::id() => State::remove(&self.state_dir),
            // `Ok(None)` — nothing was published, or a successor has already removed it — and
            // every unreadable or foreign file land here, and all of them mean "not ours".
            _ => Ok(()),
        }
    }

    /// The plane's transport, taken out of its slot under this file's one lock rule (D3): a
    /// poisoned lock is recovered with `PoisonError::into_inner`, never unwrapped and never
    /// propagated.
    ///
    /// **One method for the two sites that take it, because they must agree.** `serve` takes the
    /// transport it is about to run; `Drop` takes whatever a startup left behind. A poison flag
    /// says a lock holder panicked somewhere else, which is no statement at all about the slot —
    /// and it used to be read as one at both sites: `serve` turned it into an `Error::Internal`
    /// naming the lock, failing a legitimate start for a reason that says nothing about the plane,
    /// and `Drop` read `if let Ok(…)` and skipped the take, leaving the transport — and the
    /// listener it owns — to the field's own drop instead of releasing it before `remove_owned`.
    /// Recovering answers both: `serve` sees the slot it actually has, and `Drop` still empties it.
    fn release_transport(&self) -> Option<Transport> {
        self.transport
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

impl Drop for Plane {
    /// Best-effort backstop only — the paths that matter call [`Plane::shutdown`], and this runs
    /// the same ownership-checked deletion (`Plane::remove_owned`) one last time. Errors are
    /// deliberately swallowed: nothing may panic on the way out of a failed startup, and the
    /// transport release is `Plane::release_transport`'s, so a poisoned slot is emptied here
    /// too rather than skipped.
    fn drop(&mut self) {
        let _ = self.release_transport();
        let _ = self.remove_owned();
    }
}

/// Dispatches to the transport's own serve. **The conversion into tokio's types happens in
/// the arms below, inside the runtime** — `serve_local` and `serve_tcp` take the `std`
/// listeners Task 2 and `select` bound before it existed (and `serve_https` converts too,
/// Task 6) — which is the whole reason `Plane::bind` can be the single-instance lock at a point
/// where no runtime exists.
async fn serve_transport(
    transport: Transport,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    match transport {
        Transport::Local { channel, .. } => serve_local(channel, shared, stop).await,
        Transport::Tcp { listener, .. } => serve_tcp(listener, shared, stop).await,
        // The `tls` arm, and the one place `select`'s HTTPS selection is served: the arm is
        // compiled out with the feature, so a build without it refuses the selection rather than
        // reaching here (that refusal is `select`'s, and Task 5's test covers it).
        #[cfg(feature = "tls")]
        Transport::Https {
            listener,
            cert,
            key,
            ..
        } => serve_https(listener, &cert, &key, shared, stop).await,
    }
}

// This test module's `unreachable!`s close the tests' own control flow: `LocalForm` selections
// the tests made themselves, and a `for` loop whose exits are its `assert!` and its `return`.
// `clippy.toml`'s in-tests keys reach the unwrap/expect/panic family only — `unreachable` has no
// `allow-*-in-tests` key (design 2026-10-09 D7) — so the module carries the stated allow the rule
// calls for (docs/style.md §Panics).
#[expect(
    clippy::unreachable,
    reason = "test context: these `unreachable!`s close the tests' own control flow — `LocalForm` selections the tests made themselves and a `for` loop whose exits are its `assert!` and its `return` (docs/style.md §Panics)"
)]
#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;
    // Under `tls` the glob below also brings in tokio's `TcpStream`; the stall test wants the
    // plain blocking one, and an explicit import shadows a glob.
    #[cfg(feature = "tls")]
    use std::net::TcpStream;
    // The std listener the unix plane legs hand over, under its own name: `UnixListener` alone is
    // the tokio type through the glob below, and the two are not interchangeable.
    #[cfg(unix)]
    use std::os::unix::net::UnixListener as StdUnixListener;
    // The pipe names the management legs bind are distinct per test, so the counter is here.
    #[cfg(windows)]
    use std::sync::atomic::{AtomicU32, Ordering};
    #[cfg(unix)]
    use std::time::Duration;
    // The pipe tests' own imports: the raw dial, the silent connection the drain is asserted
    // on, and the completed-but-unclosed one the bound is. Nothing else in this module names
    // them.
    #[cfg(windows)]
    use std::time::Duration;
    // The plane legs' ready-probe and drain bounds, and the TLS stall leg's own reading: one
    // import for all three, because the default graph has all three.
    use std::time::Instant;
    // No `Arc` here: the module's own import carries it in through `super::*`, and only the unix
    // and TLS legs of this module name it.
    use std::{
        net::{SocketAddr, TcpListener},
        panic,
        path::{Path, PathBuf},
        process,
    };

    use http::Method;
    #[cfg(windows)]
    use hyper::{Request, client::conn::http1};
    use secrecy::ExposeSecret;
    use tempfile::TempDir;
    #[cfg(unix)]
    use tokio::time::sleep;
    // The pipe tests' own import: the raw dial. Nothing else in this module names it.
    #[cfg(windows)]
    use tokio::{net::windows::named_pipe::ClientOptions, time::sleep};
    // The plane legs' own two: the shutdown channel the CLI owns, the bounded drain that awaits
    // the serving task, and the task handle `Running` holds.
    use tokio::{sync::watch, task::JoinHandle, time::timeout};

    use super::*;
    // The drain test's recording device, named here rather than at its two use sites: this crate
    // reaches its shared crates through the root's `_util`/`_core` aliases (`lib.rs`, `error.rs`'s
    // test module), and clippy's `absolute_paths` wants a `use` either way.
    #[cfg(windows)]
    use crate::_util::testing::{capture, recorder};
    #[cfg(feature = "tls")]
    use crate::test_support::{fixture_pair, https_get};
    #[cfg(unix)]
    use crate::{bind::bind_unix, state, test_support::sock};
    use crate::{
        client::{self, Call, RawResponse, Report},
        router::{Auth, Context, StatusBody},
        state::State,
        test_support::{Counting, gauged},
    };

    /// A `GET` call with the token when the leg has one: every request these tests make is one,
    /// and [`client::Call`] is the shape the crate's client takes.
    fn get<'a>(target: &'a str, token: Option<&'a str>) -> client::Call<'a> {
        client::Call {
            target,
            method: Method::GET,
            token,
            body: None,
        }
    }

    /// The one channel a plane uses: the sender lives in the `Context` (`/stop` sends on it),
    /// the receiver is handed to `serve_local` — the real wiring, so the tests exercise the
    /// shape the CLI uses rather than a second one.
    #[cfg(unix)]
    fn shared() -> (Shared, watch::Receiver<bool>) {
        let state = State::new(
            "tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state");
        let (tx, rx) = watch::channel(false);
        (
            Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx)),
            rx,
        )
    }

    /// A TCP plane over `dir`, which doubles as its state dir and its root: the shape a run
    /// binds when `[api]` selects http, and the one `Plane::bind` must reach with no runtime
    /// anywhere in scope (the rule `the_plane_binds_without_a_runtime` exists to keep).
    fn plane(dir: &Path) -> Plane {
        let (stop, _rx) = watch::channel(false);
        Plane::bind(Options {
            selection: Selection {
                http: Some("127.0.0.1:0".parse().expect("addr")),
                ..Selection::default()
            },
            state_dir: dir.to_path_buf(),
            root: dir.to_path_buf(),
            host: "127.0.0.1".to_string(),
            stop,
            gauges: None,
            token: None,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        })
        .expect("a TCP bind needs no runtime")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_serves_status() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_local(channel, shared, stop));
        let status = client::get_local(&path, get("/status", Some(&token)))
            .await
            .expect("status");
        assert_eq!(status.status, 200);
        assert!(status.body.contains("\"running\":true"), "{}", status.body);

        serving.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_leaves_a_replaced_socket_alone() {
        // The bind's reclaim path must not touch a socket a live server is behind: this
        // test proves the pair — the second bind refuses rather than unlinking.
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let serving = tokio::spawn(serve_local(channel, shared, stop));
        sleep(Duration::from_millis(50)).await;

        assert!(bind_unix(&path).is_err(), "a live socket is not residue");
        serving.abort();
    }

    #[test]
    fn exactly_one_transport_may_be_enabled() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        let both = select(Selection {
            local: Some(LocalForm::Unix(PathBuf::from(
                "/tmp/tinio-0123456789abcdef.sock",
            ))),
            http: Some(addr),
            https: None,
        });
        assert!(
            both.is_err(),
            "the local channel and TCP are mutually exclusive"
        );

        let http_and_https = select(Selection {
            local: None,
            http: Some(addr),
            https: Some(HttpsOptions {
                addr,
                cert: PathBuf::from("/tmp/cert.pem"),
                key: PathBuf::from("/tmp/key.pem"),
            }),
        });
        #[cfg(feature = "tls")]
        assert!(
            http_and_https.is_err(),
            "http and https are mutually exclusive"
        );
        #[cfg(not(feature = "tls"))]
        assert!(
            http_and_https.is_err(),
            "https without the tls feature is a usage error"
        );

        let none = select(Selection {
            local: None,
            http: None,
            https: None,
        });
        assert!(none.is_err(), "no transport at all is a startup error");
    }

    /// The rule the whole task exists to keep, and the one the serving tests below cannot
    /// assert: **the bind needs no runtime.** `select` and `Plane::bind` run before the CLI's
    /// runtime exists (design §1: steps a–j precede k), which is what makes the plane the
    /// single-instance lock — so the TCP arm binds with `std` and `serve_tcp` converts inside
    /// the runtime. A plain `#[test]` has no reactor at all, so reaching for a tokio listener
    /// in either bind path stops this compiling rather than passing quietly.
    #[test]
    fn the_plane_binds_without_a_runtime() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());

        // `publish` is the readiness point and the only writer of `state` (decision 12).
        plane.publish(9000).expect("publish");
        let state = State::read(dir.path()).expect("read").expect("published");
        assert_eq!(state.port, Some(9000));
        assert!(
            state.control_name.starts_with("127.0.0.1:"),
            "the identity is the bound address: {}",
            state.control_name
        );

        // Published once: a second call is a no-op, so the file and `/status` cannot disagree.
        plane.publish(9001).expect("publish again");
        let state = State::read(dir.path()).expect("read").expect("published");
        assert_eq!(
            state.port,
            Some(9000),
            "the first port is the one `state` keeps"
        );

        // ...and the plane owns both artifacts, so `Drop` is the backstop that removes what
        // `publish` wrote on the paths that never reach `shutdown`.
        drop(plane);
        assert!(
            State::read(dir.path()).expect("read").is_none(),
            "dropping the plane removes the state file it published"
        );
    }

    /// The D3 rule at both sites that take the plane's transport, pinned because each of them
    /// used to read the poison flag as a fact about the plane: `serve` propagated it as an
    /// `Error::Internal` naming the lock, and `Drop` skipped the take outright.
    ///
    /// **`serve`'s half, and the slot is emptied first so the answer says which code ran.** A
    /// recovered guard finds `None` and refuses with "already serving"; the propagation this
    /// replaced answered about the lock instead, which would have failed a legitimate start for a
    /// reason that names neither the plane nor its state.
    #[tokio::test]
    async fn a_poisoned_transport_lock_still_refuses_the_second_serve() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());
        assert!(
            plane.release_transport().is_some(),
            "the bind leaves the transport in the slot"
        );
        poison(&plane.transport);
        let (_stop, rx) = watch::channel(false);
        let err = plane
            .serve(rx)
            .await
            .expect_err("a plane with no transport left cannot serve");
        assert!(
            matches!(&err, Error::Internal(message) if message == "the plane is already serving"),
            "the recovered lock reaches the take: {err:?}"
        );
    }

    /// **`Drop`'s half, and it is the take itself.** `if let Ok(…)` skipped it on a poisoned
    /// lock, so a plane dropped mid-startup kept its transport — and the listener under it —
    /// alive past the backstop that exists to release it. `release_transport` is the body `Drop`
    /// runs, so this is that path.
    #[test]
    fn a_poisoned_transport_lock_still_yields_the_transport() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());
        poison(&plane.transport);
        assert!(
            plane.release_transport().is_some(),
            "the poisoned lock still hands the transport over"
        );
    }

    /// Poisons `lock` the way a panicking holder does — the guard is dropped while a thread
    /// unwinds through it. The panic is caught and asserted, so the poisoning is a fact this
    /// helper establishes rather than a failure of the test that asked for it.
    fn poison(lock: &Mutex<Option<Transport>>) {
        let unwound = panic::catch_unwind(|| {
            let _guard = lock.lock().expect("a fresh lock");
            panic!("poison the lock");
        });
        assert!(unwound.is_err(), "the poisoning panic was caught");
    }

    /// **The two answers `state_for` has, and why the difference is load-bearing.** A plane that
    /// supplies a token is the confined child of a launcher that already published `state` with
    /// that token: minting a fresh one here would leave every client that found the server
    /// refused `401` by it. A plane that supplies none owns its `state` file, and generates the
    /// token it is about to write into it.
    #[test]
    fn a_supplied_token_is_the_one_the_plane_serves_with() {
        let dir = TempDir::new().expect("temp dir");
        let token = "ab".repeat(32);
        let adopted = state_for(Some(token.clone()), "127.0.0.1:9001", dir.path()).expect("state");
        assert_eq!(
            adopted.token.expose_secret().as_str(),
            token.as_str(),
            "the caller's token, verbatim"
        );

        let own = state_for(None, "127.0.0.1:9001", dir.path()).expect("state");
        assert_eq!(
            own.token.expose_secret().len(),
            64,
            "a fresh token, as `State::new` writes"
        );
        assert_ne!(own.token.expose_secret().as_str(), token.as_str());
    }

    /// The gate is released before the deletions: `serve` returns the moment the shared channel
    /// flips, and the CLI still has the data plane's drain to await before it calls `shutdown`
    /// (decision 12's sequence). A successor that passes the gate in that window has published
    /// its own `state`, so the predecessor must remove the file only while it is still its own.
    ///
    /// **This is also the launcher-written file's test, and the pid is what makes it one.** The
    /// confined child's plane reads a `state` file that names the launcher — not itself — and
    /// this is that same shape: the guard holds, so `shutdown` leaves the file alone and the
    /// child never even attempts the deletion its confinement would refuse.
    #[tokio::test]
    async fn a_state_file_from_another_process_survives_shutdown() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());

        let mut successor = State::new("127.0.0.1:9001", Some(9001), dir.path()).expect("state");
        successor.pid = process::id().wrapping_add(1);
        successor.write(dir.path()).expect("write");

        plane.shutdown().await.expect("shutdown");

        let kept = State::read(dir.path()).expect("read").expect("still there");
        assert_ne!(
            kept.pid,
            process::id(),
            "a successor's state file is not this plane's to remove"
        );
    }

    /// The socket half of the same rule: a successor's `bind_unix` reclaims the path by
    /// unlinking it and binding again, which leaves a different file at the same name. The
    /// predecessor must not unlink *that* — deleting a live server's socket would break
    /// discovery and, with the path free again, the single-instance gate with it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_replaced_socket_file_survives_shutdown() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("tinio-0123456789abcdef.sock");
        let (stop, _rx) = watch::channel(false);
        let plane = Plane::bind(Options {
            selection: Selection {
                local: Some(LocalForm::Unix(path.clone())),
                ..Selection::default()
            },
            state_dir: dir.path().to_path_buf(),
            root: dir.path().to_path_buf(),
            host: "127.0.0.1".to_string(),
            stop,
            gauges: None,
            token: None,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        })
        .expect("bind");
        assert!(path.exists(), "the bind created the socket file");

        // What the successor leaves behind: the same name, a different file — and therefore a
        // different inode, which is the whole reason the deletion checks identity rather than
        // the path.
        fs::remove_file(&path).expect("remove the bound socket");
        fs::write(&path, b"a successor's socket").expect("replace");

        plane.shutdown().await.expect("shutdown");

        assert!(
            path.exists(),
            "a successor's socket file is not this plane's to remove"
        );
    }

    #[tokio::test]
    async fn a_loopback_tcp_plane_does_not_guard_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let (shared, _) = gauged(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        // A loopback bind has the local channel's trust level, so `/metrics` is open there:
        // the scrape reaches the handler over a real socket and answers the concrete `200`
        // (Task 7 registered the route, so the token-less request is no longer "anything but
        // a 401" — an unrouted path would have answered `404`).
        let open = client::get_http(addr, get("/metrics", None))
            .await
            .expect("request");
        assert_eq!(open.status, 200, "a loopback bind keeps /metrics open");
        let guarded = client::get_http(addr, get("/status", None))
            .await
            .expect("request");
        assert_eq!(
            guarded.status, 401,
            "/status still needs the token on a loopback bind"
        );
        let allowed = client::get_http(addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }

    #[tokio::test]
    async fn a_non_loopback_tcp_plane_requires_the_token() {
        // The rule follows the *bind* address, so a wildcard bind is the strict one; the
        // client still dials 127.0.0.1, because a wildcard is not a connect address.
        let listener = TcpListener::bind("0.0.0.0:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let bound = listener.local_addr().expect("addr");
        let addr = SocketAddr::from(([127, 0, 0, 1], bound.port()));
        let state = State::new(
            bound.to_string(),
            Some(bound.port()),
            Path::new("/srv/data"),
        )
        .expect("state");
        let (tx, stop) = watch::channel(false);
        let (shared, _) = gauged(Context::new(state, "0.0.0.0", Auth::for_bind(bound), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        // Every registered endpoint, `/metrics` included: a wildcard bind is the strict one, so
        // the scrape needs the token here even though the client dials loopback. The source is
        // attached, which is what makes the 401 the guard's answer rather than a `404` from an
        // unrouted path (an unrouted path never reaches the middleware — the guard is a
        // `route_layer`, `router.rs`).
        let guarded = client::get_http(addr, get("/status", None))
            .await
            .expect("request");
        assert_eq!(
            guarded.status, 401,
            "a non-loopback bind guards every registered endpoint"
        );
        let guarded_metrics = client::get_http(addr, get("/metrics", None))
            .await
            .expect("request");
        assert_eq!(
            guarded_metrics.status, 401,
            "/metrics is guarded on a non-loopback bind too"
        );
        let tokened_metrics = client::get_http(addr, get("/metrics", Some(&token)))
            .await
            .expect("request");
        assert_eq!(tokened_metrics.status, 200, "the token opens the scrape");
        let allowed = client::get_http(addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }

    /// The HTTPS transport end to end: the `std` listener `select` produces, `serve_https`
    /// converting it inside the runtime, and a rustls client reading `/status` back over a real
    /// handshake — against the committed fixture pair, so nothing is generated at test time on any
    /// platform.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn https_serves_status_over_tls() {
        let (cert, key) = fixture_pair();
        // Bound as `std` and handed over unconverted: that is the shape `select` produces and
        // `serve_https` consumes, and converting here would test a wiring that does not exist.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");

        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        // The pair is moved into the spawned future rather than borrowed from the test body:
        // `tokio::spawn` wants `'static`, and a `&cert`/`&key` here would outlive nothing.
        let serving =
            tokio::spawn(async move { serve_https(listener, &cert, &key, shared, stop).await });
        let answer = https_get(&addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(answer.status, 200, "{}", answer.body);

        serving.abort();
    }

    /// The property [`HANDSHAKE_TIMEOUT`] exists for, and the one the happy path cannot show: a
    /// client that connects and then says nothing must not take the accept loop with it.
    ///
    /// The bare socket below never sends a ClientHello, so its handshake can never complete, and
    /// `accept` is where that handshake runs — so without the timeout this test hangs rather than
    /// failing, which is exactly what a stalled client would do to `/status`, `/stop` and the
    /// readiness probe in production. With it, the silent connection is released and a real client
    /// is answered while the test's end of that socket is still open — the test never closes it,
    /// which is what makes the answer evidence that the loop was freed and not merely that the
    /// test gave up waiting.
    ///
    /// **The elapsed time is reported, not asserted on**: a five-second bound is a liveness floor
    /// ([`HANDSHAKE_TIMEOUT`]), and asserting a duration here would be a timing test that fails on
    /// a loaded machine.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn a_silent_connection_does_not_stall_the_tls_listener() {
        let (cert, key) = fixture_pair();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");

        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving =
            tokio::spawn(async move { serve_https(listener, &cert, &key, shared, stop).await });

        // Connected and silent: the accept loop is inside this one's handshake until the timeout
        // drops it, so nothing else is accepted in the meantime.
        let silent = TcpStream::connect(addr).expect("connect the silent socket");

        let started = Instant::now();
        let answer = https_get(&addr, get("/status", Some(&token)))
            .await
            .expect("request");
        let elapsed = started.elapsed();
        assert_eq!(answer.status, 200, "{}", answer.body);
        eprintln!("stall regression: /status answered in {elapsed:?} with a silent socket open");
        // Closed only here, after the assertion, and that ordering is the whole proof: `/status`
        // was answered while this connection was still open, because the test never closed it.
        // Nothing here can assert the socket's liveness from this side — the timeout closes the
        // *server's* end, so a peer check or a read would succeed or fail either way and would
        // prove nothing about what released the loop.
        drop(silent);
        serving.abort();
    }

    /// **The pool answers every dial, back to back, with no client retry.** This is the property
    /// the confined run's `status`/`stop` depend on: every client dials the one name, and
    /// `bind_pipe` binds a pool, so while one instance is connected another is listening. The
    /// retry in [`dial`] exists only to *measure* that — it is asserted to be zero, which is the
    /// count the rejected single-instance reuse could never reach (it needed retries on every run,
    /// and the next dial was killed mid-request when it did not get one).
    ///
    /// The stop half is the drain: a connection established and silent when the channel flips is
    /// still answered, and only after the client lets go does the serve future return. That is the
    /// property the previous single-instance loop got from its `JoinSet`, now through one task per
    /// instance — the serving task is not the one watching the shutdown channel.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_pipe_pool_serves_back_to_back_dials_with_no_gap() {
        const DIALS: usize = 200;
        let name = bind::pipe_name(&format!(r"\\.\pipe\tinio-pool-{}", process::id()));
        // The production bind, pool included: its instances are what `serve_local` rotates.
        let channel = bind::bind_pipe(&name).expect("bind");
        let (tx, rx) = watch::channel(false);
        let state = State::new(name.clone(), Some(9000), Path::new("/srv/data")).expect("state");
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx.clone()));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_local(channel, shared, rx));
        let mut retried = 0usize;
        for nth in 1..=DIALS {
            let (answer, attempts) = dial(&name, &token, nth).await;
            retried += attempts;
            assert_eq!(answer.status, 200, "dial {nth}: {}", answer.body);
            assert!(
                answer.body.contains("\"running\":true"),
                "dial {nth}: {}",
                answer.body
            );
        }
        assert_eq!(
            retried, 0,
            "a dial landed in a gap the instance pool exists to close"
        );
        eprintln!("pipe pool: {DIALS} sequential dials, {retried} retries");

        // A connection established and silent when the stop fires.
        let stream = ClientOptions::new()
            .open(&name)
            .expect("dial the control pipe");
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .expect("handshake");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // The instance whose `connect` this dial completed has been handed the connection by now.
        sleep(Duration::from_millis(100)).await;
        tx.send(true).expect("the instance loops are listening");
        // **The drain is holding that connection**, and this is the assertion the retired
        // `tests/management.rs` leg contributed here (`plans/2026-10-01-test-placement-cleanup.md`):
        // `serve_local` cannot return while a connection it accepted is still in flight, so the
        // stop has not taken the serving task down with the accept loop.
        assert!(
            !serving.is_finished(),
            "the drain abandoned the connection the loop had accepted"
        );

        let request = Request::builder()
            .method("GET")
            .uri("/status")
            .header("x-tinio-token", &token)
            .body(String::new())
            .expect("request");
        let response = sender
            .send_request(request)
            .await
            .expect("the drain answers the connection that was open when the stop fired");
        assert_eq!(
            response.status().as_u16(),
            200,
            "the open connection was abandoned rather than drained"
        );
        // Only when the client lets go does the serving instance's loop see the stop — and only
        // then does `serve_local` return, which is the drain this awaits rather than aborts.
        drop(sender);
        serving
            .await
            .expect("the serve task ends rather than panicking")
            .expect("serve returns Ok");
    }

    /// Writes one canned HTTP request into the raw pipe, in a `poll_write` loop: the crate's tokio
    /// graph carries no `io-util`, so there is no `AsyncWriteExt` here (`client.rs`'s R29 test
    /// states the same fact), and this is the whole of what one request needs.
    #[cfg(windows)]
    async fn write_request(pipe: &mut (impl AsyncWrite + Unpin), mut bytes: &[u8]) {
        poll_fn(|cx| {
            while !bytes.is_empty() {
                match Pin::new(&mut *pipe).poll_write(cx, bytes) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::other(
                            "the pipe stopped accepting the request",
                        )));
                    }
                    Poll::Ready(Ok(written)) => bytes = &bytes[written..],
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            Poll::Ready(Ok(()))
        })
        .await
        .expect("the request reaches the server");
    }

    /// **A client that never closes holds its instance only under the bounded drain** — §5's
    /// second scenario, and the measurement [`DRAIN_LIMIT`] and [`wait_for_peer_close`] state.
    ///
    /// Every instance is answered and then left open — the client writes a request and then neither
    /// reads nor closes — so each serving task sits in [`wait_for_peer_close`] until the bound,
    /// reclaims its instance and reports the reclaim; the pool then serves a fresh dial again. That
    /// last fact is the property the bound exists for: unbounded, a client that never lets go would
    /// hold its instance for as long as it lived, and every instance held at once is the name gone
    /// deaf.
    ///
    /// **The client is raw and speaks HTTP by hand, and that is forced rather than chosen**: a held
    /// `SendRequest` does not keep a pipe open (measured: the server released the instance ~100 µs
    /// after the exchange, because the handle lives in hyper's connection future), so a client
    /// built out of `hyper` cannot be the client this scenario is about. The flavor is the other
    /// half of what made the scenario untestable: the dial path's `ClientOptions::open` blocks the
    /// calling thread (`plans/2026-09-25-sandbox-followups.md` §5).
    ///
    /// **What the loop waits for is a `200`, not "an answer", and the reclaim is why.** A reclaim
    /// is a cancellation: the drain's read is dropped at the bound, and the instance that comes
    /// back carries the runtime's own read state into the next connection — measured *before* the
    /// drain defect was fixed, as this test's own transcript: the first dial after the reclaims
    /// ends in `Transport("/status: connection error")` (the error the disarm gives the read it
    /// cancelled), the dials after it are answered `405 Method Not Allowed` because the request
    /// reached the router as `ET /status HTTP/1.1` with its first byte taken by the drain's
    /// one-byte read, and the pool only converges on a clean `200` after a dozen to sixty further
    /// dials. Two rules in the implementation are what removed that ([`wait_for_peer_close`] reads
    /// a whole chunk and answers whether the client's close ended the wait; [`Pool::poll_read`]
    /// retries a pre-first-byte error once on an instance that came back unclean), and **the
    /// assertion below pins both**: with the retry disabled and the drain's buffer back at one byte
    /// the test fails with exactly that transcript (measured 2026-09-28).
    ///
    /// The dials that cannot *connect* are waited through rather than counted — `ERROR_PIPE_BUSY`
    /// while both instances are still held or being disarmed is the pool working, not a dial — so
    /// the budget is that wait's: 250 ticks of 20 ms, five seconds, against a bound of 250 ms.
    // **`recorder`'s guard is held across this test's awaits on purpose**, which is the one thing
    // the lint cannot see: the guard is the process-wide serialisation of every-thread recordings,
    // and the recording it protects is the whole test, so there is no earlier point at which it
    // could be dropped.
    #[expect(
        clippy::await_holding_lock,
        reason = "test context: `recorder`'s guard is held across this test's awaits on purpose — it is the process-wide serialisation of every-thread recordings and the recording it protects is the whole test, so there is no earlier point at which it could be dropped"
    )]
    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_that_never_closes_holds_its_instance_only_under_the_bounded_drain() {
        let name = bind::pipe_name(&format!(r"\\.\pipe\tinio-drain-{}", process::id()));
        let channel = bind::bind_pipe(&name).expect("bind");
        let (tx, rx) = watch::channel(false);
        let state = State::new(name.clone(), Some(9000), Path::new("/srv/data")).expect("state");
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx.clone()));
        let token = shared.state.token.expose_secret().as_str().to_owned();
        let serving = tokio::spawn(serve_local(channel, shared, rx));

        // The reclaim's own event, recorded across threads: the warning is emitted by a serving
        // task, never by this one.
        let capture = capture();
        let _serialise = recorder();
        let recording = capture.record_every_thread();

        // Both instances claimed and answered, then held rather than dialled and released: the
        // handles are the pool's occupancy, so what the drain waits on is real. The request is
        // written by hand because the client that has to hold is a client that speaks HTTP itself,
        // and nothing is read afterwards — holding the connection is the whole scenario.
        let request =
            format!("GET /status HTTP/1.1\r\nhost: 127.0.0.1\r\nx-tinio-token: {token}\r\n\r\n");
        let mut held = Vec::new();
        for _ in 0..bind::INSTANCES {
            let mut pipe = ClientOptions::new().open(&name).expect("take an instance");
            write_request(&mut pipe, request.as_bytes()).await;
            held.push(pipe);
        }

        // **The first dial that connects is answered `200`, and every dial of the two that follow
        // it is too** — the property the bound is for, and the one the old code could not reach:
        // its reclaimed instances answered a connection error and then a previous request's tail
        // (`405 Method Not Allowed`, the request parsed as `ET /status HTTP/1.1`). Dials that
        // cannot *connect* are not dials at all and are waited through — `ERROR_PIPE_BUSY` while
        // both instances are still held or being re-armed is the pool working — and the budget is
        // that wait's, five seconds of 20 ms ticks rather than a count tuned to the pool.
        // What a failing dial saw is kept so the assertion can show the whole transcript.
        let mut transcript = Vec::new();
        let mut dials = 0usize;
        let mut answer = None;
        for _ in 0..250 {
            // Not being able to *connect* is not a dial: `ERROR_PIPE_BUSY` while both instances
            // are still held (or between a disarm and its re-arm) is the pool working, and it is
            // waited through rather than counted.
            let Ok(stream) = ClientOptions::new().open(&name) else {
                sleep(Duration::from_millis(20)).await;
                continue;
            };
            // From here on it is a dial, and every dial has to be answered: a connection that is
            // taken and then answered nothing, or answered something other than `/status`, is the
            // instance the bound handed back putting its own read state in front of the request.
            let reply = client::exchange(TokioIo::new(stream), get("/status", Some(&token)))
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "dial {} was answered nothing at all: {err}. Transcript: {transcript:#?}",
                        dials + 1
                    )
                });
            dials += 1;
            transcript.push(format!("{} {}", reply.status, reply.body));
            assert_eq!(
                reply.status, 200,
                "dial {dials} after the reclaims was not answered `/status`. Transcript: \
                 {transcript:#?}"
            );
            answer = Some(reply);
            if dials == 3 {
                break;
            }
        }
        let answer = answer.expect("the bound reclaims the instances, so the pool serves again");
        assert!(answer.body.contains("\"running\":true"), "{}", answer.body);
        eprintln!("pipe drain: {dials} dials answered /status after the reclaims");

        drop(recording);
        // Every held instance is a reclaim of its own, and the identity is part of what is asserted:
        // an every-thread recording collects the other tests' events too, and this crate's pipe
        // tests share the one warning text.
        let logged = capture.text();
        let reclaims = logged
            .lines()
            .filter(|line| line.contains("reclaiming the pipe instance") && line.contains(&name))
            .count();
        assert!(
            reclaims >= bind::INSTANCES,
            "each of the {INSTANCES} held instances reports its own forced reclaim, and {reclaims} \
             were reported: {logged}",
            INSTANCES = bind::INSTANCES
        );

        drop(held);
        serving.abort();
    }

    /// The same sequential dials through `Plane`: the path the plane's own legs above drive, kept
    /// here so a regression in `Plane::serve`'s wiring is diagnosable without the drained legs'
    /// setup.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_plane_serves_sequential_pipe_connections() {
        let dir = TempDir::new().expect("temp dir");
        let name = bind::pipe_name(&format!(r"\\.\pipe\tinio-plane-{}", process::id()));
        let (stop, rx) = watch::channel(false);
        let plane = Arc::new(
            Plane::bind(Options {
                selection: Selection {
                    local: Some(LocalForm::Pipe(name.clone())),
                    ..Selection::default()
                },
                state_dir: dir.path().to_path_buf(),
                root: dir.path().to_path_buf(),
                host: "127.0.0.1".to_string(),
                stop,
                gauges: None,
                token: None,
                #[cfg(feature = "lifecycle")]
                lifecycle: None,
            })
            .expect("bind"),
        );
        plane.publish(9000).expect("publish");
        let token = plane.shared.state.token.expose_secret().as_str().to_owned();
        let serving = tokio::spawn({
            let plane = Arc::clone(&plane);
            async move { plane.serve(rx).await }
        });

        for nth in 1..=4 {
            let (answer, _) = dial(&name, &token, nth).await;
            assert_eq!(answer.status, 200, "dial {nth}: {}", answer.body);
        }
        // The crate's own client, twice: the two steps the plane's drained legs take through the
        // same client, which is where the raw dials above and the CLI's own path diverge.
        for nth in 1..=2 {
            let report = client::status(dir.path()).await.expect("status reads");
            assert!(report.running, "client status {nth} did not report running");
        }
        assert!(!serving.is_finished(), "the plane is still serving");
        serving.abort();
    }

    /// One dial of `name`, retrying a connection that lands in a gap. **Zero retries is the
    /// expected count**: the bind's pool means a listening instance is always there, so the retry
    /// exists only to make a regression a reported number rather than a broken pipe — and to make
    /// this helper assert nothing about the loop it is measuring.
    ///
    /// The crate's own pipe client makes a single `open` with no wait — the right shape for a
    /// `status` probe, which is why any waiting belongs to this test.
    #[cfg(windows)]
    async fn dial(name: &str, token: &str, nth: usize) -> (client::RawResponse, usize) {
        use tokio::{net::windows::named_pipe::ClientOptions, task::yield_now};

        for attempt in 0.. {
            assert!(
                attempt < 1_000,
                "dial {nth}: the pool never had a listening instance"
            );
            if let Ok(stream) = ClientOptions::new().open(name) {
                match client::exchange(TokioIo::new(stream), get("/status", Some(token))).await {
                    Ok(answer) => return (answer, attempt),
                    // The connection was killed between the open and the answer: the gap again.
                    Err(_) => continue,
                }
            }
            // Every instance busy: `ERROR_PIPE_BUSY`, which is momentary with a pool — the
            // instances' `connect` calls run on this very runtime. Yielding is what lets them.
            yield_now().await;
        }
        unreachable!("the loop above returns or asserts")
    }

    // ---------------------------------------------------------------------------------------
    // The plane's own legs, moved here from `tests/management.rs` (which is retired with them,
    // `plans/2026-10-01-test-placement-cleanup.md`). Their subject is this module's own
    // sequence — `Plane::bind(options)` → `publish(port)` → `serve(stop)` → the status/stop
    // client → `shutdown()` — plus the OS state a live run has to be read back for: the socket
    // file, the pipe's single-instance refusal, and the local channel dialled the
    // way the CLI dials it.
    //
    // The double the gauges' half uses is [`crate::test_support::Counting`], the crate's one
    // definition of those counts, and its reader is driven through `Plane::serve` exactly as the
    // CLI drives it.
    // ---------------------------------------------------------------------------------------

    /// How long the CLI gives a drain before it stops waiting (design decision 12's bound): the
    /// tests use the same number, so a hang is reported rather than absorbed.
    const DRAIN_BOUND: Duration = Duration::from_secs(10);

    /// How long a leg waits for its plane to answer: `state` is published at `publish`, *before*
    /// `serve` reaches its accept loop, and the local channel's backlog can be one connection deep.
    const READY_BOUND: Duration = Duration::from_secs(5);

    /// The CLI's selection for one resolved local channel: the caller resolved the form (the
    /// config's `[api.unix]`/`[api.pipe]` section, or the derived default), and nothing downstream
    /// re-derives a name from it.
    fn local_selection(local: &LocalForm) -> Selection {
        Selection {
            local: Some(local.clone()),
            ..Selection::default()
        }
    }

    /// The two directories a leg serves from: the state dir the plane anchors on, and the served
    /// root — canonical, because it is what the socket name hashes and what `state` records.
    struct Dirs {
        state_dir: TempDir,
        /// Held for its lifetime alone: `root` is the canonical path *under* this directory, and
        /// dropping the guard would delete it out from under the plane.
        _root_dir: TempDir,
        root: PathBuf,
    }

    impl Dirs {
        fn new() -> Self {
            let state_dir = TempDir::new().expect("state dir");
            let root_dir = TempDir::new().expect("root");
            let root = root_dir.path().canonicalize().expect("canonical root");
            Self {
                state_dir,
                _root_dir: root_dir,
                root,
            }
        }

        fn dir(&self) -> &Path {
            self.state_dir.path()
        }

        fn root(&self) -> &Path {
            &self.root
        }

        /// The local channel a run over these two directories binds when the config names
        /// neither: the derived unix socket, whose name is a function of the root
        /// (`state::socket_path`), or a pipe name distinct per test.
        #[cfg(unix)]
        fn local(&self) -> LocalForm {
            LocalForm::Unix(state::socket_path(self.dir(), self.root()).expect("socket path"))
        }

        #[cfg(windows)]
        fn local(&self) -> LocalForm {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            // The full `\\.\pipe\…` form, which is what a client dials and what `bind_pipe` records:
            // the bare spelling is the config's, and the bind's own normalisation is what turns it into
            // this — the leg hands it the already-normalised one so the name it later dials is
            // byte-for-byte the one the plane bound.
            LocalForm::Pipe(format!(
                r"\\.\pipe\tinio-test-{}-{}",
                process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
        }

        /// The bound control socket, unix only.
        #[cfg(unix)]
        fn socket(&self) -> PathBuf {
            let LocalForm::Unix(path) = self.local() else {
                unreachable!("the derived local form is a socket path");
            };
            path
        }
    }

    /// A plane that is bound, published and serving, with the shutdown channel the CLI owns.
    struct Running {
        plane: Arc<Plane>,
        serving: JoinHandle<Result<(), Error>>,
        /// The channel's own end, for the leg that stops the plane without the client. Both are
        /// the Windows channel's — [`Running::end`] is the field's only reader, and it is
        /// `#[cfg(windows)]` with the stop leg that drives it — so the field carries the same
        /// gate rather than reading as dead code on the platforms that never stop this way.
        #[cfg(windows)]
        stop: watch::Sender<bool>,
        rx: watch::Receiver<bool>,
        report: Report,
    }

    impl Running {
        /// The CLI's own startup, in order: bind (the single-instance lock) → publish (the
        /// readiness point) → serve, then wait until the channel answers.
        ///
        /// **One body for both legs, because the two differ in the bind and in nothing else**, and
        /// `bind` is that difference: `Plane::bind` for the CLI's own path, `Plane::adopt` for the
        /// confined child's. The plane is built with `token: None` — this harness's plane owns its
        /// `state` file, so it generates its own token, and the launcher-supplied case is the
        /// confined child's.
        async fn start_with(
            dirs: &Dirs,
            selection: Selection,
            gauges: Option<Arc<Gauges>>,
            bind: impl FnOnce(Options) -> Result<Plane, Error>,
        ) -> Self {
            let (stop, rx) = watch::channel(false);
            let plane = Arc::new(
                bind(Options {
                    selection,
                    state_dir: dirs.dir().to_path_buf(),
                    root: dirs.root().to_path_buf(),
                    host: "127.0.0.1".to_string(),
                    stop: stop.clone(),
                    gauges,
                    token: None,
                    #[cfg(feature = "lifecycle")]
                    lifecycle: None,
                })
                .expect("the plane is built"),
            );
            plane.publish(9000).expect("publish");
            let serving = tokio::spawn({
                let plane = Arc::clone(&plane);
                let rx = rx.clone();
                async move { plane.serve(rx).await }
            });

            await_ready(dirs.dir()).await;
            // Read back through the client the CLI uses.
            let report = client::status(dirs.dir()).await.expect("state reads");
            assert!(report.state.is_some(), "`publish` wrote the state file");
            Self {
                plane,
                serving,
                #[cfg(windows)]
                stop,
                rx,
                report,
            }
        }

        /// [`Running::start_with`] on [`Plane::bind`]: the CLI's own single-instance path.
        async fn start(dirs: &Dirs, selection: Selection, gauges: Option<Arc<Gauges>>) -> Self {
            Self::start_with(dirs, selection, gauges, Plane::bind).await
        }

        /// [`Running::start_with`]'s **adopted** twin, and the shape a confined child runs: the
        /// launcher already bound the control channel, and this plane serves on the socket it was
        /// handed (`Plane::adopt`) instead of binding one of its own.
        ///
        /// The launcher-token half of the same path (`state` written by the launcher, the child
        /// verifying against the value in it) is what the macOS end-to-end leg drives with a real
        /// confining launcher.
        #[cfg(unix)]
        async fn start_adopted(
            dirs: &Dirs,
            selection: Selection,
            inherited: StdUnixListener,
        ) -> Self {
            Self::start_with(dirs, selection, None, |options| {
                Plane::adopt(options, inherited)
            })
            .await
        }

        fn token(&self) -> String {
            self.report
                .state
                .as_ref()
                .expect("the state file it read")
                .token
                .expose_secret()
                .as_str()
                .to_owned()
        }

        fn status(&self) -> &StatusBody {
            self.report.status.as_ref().expect("the contract's body")
        }

        /// The CLI's stop path — `client::stop` returns `Ok` only for the contract's `202` — then
        /// the drain and the plane's own deletions.
        async fn stop(self, dirs: &Dirs) {
            client::stop(dirs.dir()).await.expect("stop answers 202");
            self.drain(dirs).await;
        }

        /// The same shutdown from the channel's own end, for the leg that drives the raw channel
        /// rather than the client's: `/stop` sends on this very sender. Windows-only, with the
        /// field and the DACL leg that is its one caller.
        #[cfg(windows)]
        async fn end(self, dirs: &Dirs) {
            self.stop.send(true).expect("the plane is listening");
            self.drain(dirs).await;
        }

        /// The bounded drain, the flipped channel, and `shutdown`'s deletion of the state file.
        async fn drain(self, dirs: &Dirs) {
            timeout(DRAIN_BOUND, self.serving)
                .await
                .expect("the drain is bounded")
                .expect("the serve task does not panic")
                .expect("serve returns Ok");
            assert!(
                *self.rx.borrow(),
                "`/stop` flips the channel `serve` awaited"
            );
            self.plane.shutdown().await.expect("shutdown");
            assert!(
                State::read(dirs.dir()).expect("read").is_none(),
                "`shutdown` removes the state file"
            );
        }
    }

    /// Whether the plane answers yet: `state` is published at `publish`, *before* `serve` reaches
    /// its accept loop, so a probe that races it answers "stopped".
    async fn answers(dir: &Path) -> bool {
        client::status(dir).await.expect("state reads").running
    }

    /// Waits, bounded, for [`answers`]. The CLI's own path — read `state`, dial the channel it
    /// names — which is what every leg in this section drives.
    async fn await_ready(dir: &Path) {
        let deadline = Instant::now() + READY_BOUND;
        while Instant::now() < deadline {
            if answers(dir).await {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("the plane did not answer within {READY_BOUND:?}");
    }

    /// One request over the local channel, with or without the token — the crate's own client
    /// covers the CLI's paths, this wrapper the rest: a tokenless `/metrics`, and `/status` with
    /// the token absent or wrong. The dial and the exchange are both the crate's
    /// ([`client::get_local`]), so there is one definition of each and not two.
    #[cfg(unix)]
    async fn local_request(
        local: &LocalForm,
        target: &str,
        method: Method,
        token: Option<&str>,
    ) -> RawResponse {
        let LocalForm::Unix(path) = local else {
            unreachable!("the unix leg dials a socket path");
        };
        client::get_local(
            path,
            Call {
                target,
                method,
                token,
                body: None,
            },
        )
        .await
        .expect("request")
    }

    /// The named-pipe form of [`local_request`]. No client helper dials a pipe — `get_local` is the
    /// unix path and refuses on this platform — so the dial is here and the exchange is the crate's
    /// one, [`client::exchange`].
    #[cfg(windows)]
    async fn local_request(
        local: &LocalForm,
        target: &str,
        method: Method,
        token: Option<&str>,
    ) -> RawResponse {
        let LocalForm::Pipe(name) = local else {
            unreachable!("the Windows leg dials a pipe name");
        };
        let stream = ClientOptions::new()
            .open(name)
            .expect("dial the control pipe");
        client::exchange(
            TokioIo::new(stream),
            Call {
                target,
                method,
                token,
                body: None,
            },
        )
        .await
        .expect("request")
    }

    /// The plane's source: the crate's [`Counting`] double, at the ttl the leg needs.
    fn gauges(source: &Arc<Counting>, ttl: Duration) -> Arc<Gauges> {
        // The clone keeps the double's concrete type so a leg can read its counters back; the
        // coercion to the trait object happens at the argument, which is where the seam is.
        let source = Arc::clone(source);
        Arc::new(Gauges::new(source, ttl))
    }

    #[tokio::test]
    async fn the_cli_sequence_answers_status_and_leaves_nothing_behind() {
        let dirs = Dirs::new();
        let local = dirs.local();
        let running = Running::start(&dirs, local_selection(&local), None).await;
        assert!(running.report.running, "the plane answers for its own root");

        let status = running.status();
        assert_eq!(status.pid, process::id());
        assert_eq!(status.port, Some(9000));
        assert_eq!(status.host, "127.0.0.1");
        assert_eq!(status.root, dirs.root().to_string_lossy());
        let token = running.token();
        assert_eq!(token.len(), 64, "the token is 32 CSPRNG bytes, hex-encoded");

        // With the token, the contract's `200`; without one, and with a wrong one, `401` — `/status`
        // is guarded on every transport, the local channel included.
        let answer = local_request(&local, "/status", Method::GET, Some(&token)).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
        assert!(answer.body.contains("\"running\":true"), "{}", answer.body);
        for absent in [None, Some("wrong")] {
            let answer = local_request(&local, "/status", Method::GET, absent).await;
            assert_eq!(answer.status, 401, "`/status` needs the token here too");
        }

        // The CLI's stop path, the drain it starts, and the plane's own deletions.
        running.stop(&dirs).await;
        #[cfg(unix)]
        assert!(
            !dirs.socket().exists(),
            "`shutdown` removes the socket file: {}",
            dirs.socket().display()
        );
    }

    #[tokio::test]
    async fn the_reader_starts_with_the_plane_and_stops_with_it() {
        let dirs = Dirs::new();
        let local = dirs.local();
        let source = Arc::new(Counting::default());
        let running = Running::start(
            &dirs,
            local_selection(&local),
            Some(gauges(&source, Duration::from_millis(20))),
        )
        .await;

        // Advanced while serving, and with no scrape at all: the reader scans once at start and then
        // once per ttl.
        sleep(Duration::from_millis(120)).await;
        let scanned = source.scans();
        assert!(
            scanned >= 2,
            "the reader scanned with the plane and kept its cadence: {scanned}"
        );

        running.stop(&dirs).await;

        // Frozen after the plane stopped, which is what this leg can show. A grace window first,
        // because the task stops on its own wake-up; *why* it stopped (the shared channel) is
        // `metrics.rs`'s unit test's assertion, not this one's.
        sleep(Duration::from_millis(60)).await;
        let frozen = source.scans();
        sleep(Duration::from_millis(120)).await;
        assert_eq!(
            source.scans(),
            frozen,
            "the reader stops with the plane rather than scanning on"
        );
    }

    /// The unix stale-socket probe-then-reclaim, driven through the plane: a file left by a crashed
    /// instance must not block a restart, and what the bind then records is the socket it bound.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_stale_socket_file_is_reclaimed_by_the_bind() {
        let dirs = Dirs::new();
        let socket = dirs.socket();
        // What a crashed instance leaves: the socket file with no listener behind it.
        let stale = StdUnixListener::bind(&socket).expect("stale socket");
        drop(stale);
        assert!(socket.exists(), "the residue is there before the bind");

        let local = LocalForm::Unix(socket.clone());
        let running = Running::start(&dirs, local_selection(&local), None).await;
        assert_eq!(
            running.report.state.as_ref().expect("state").control_name,
            socket.to_string_lossy(),
            "`state` records the socket the bind created"
        );

        running.stop(&dirs).await;
        assert!(!socket.exists(), "and `shutdown` removes it");
    }

    /// **The adopted half of that same channel**, and the property the macOS profile now depends on:
    /// the launcher binds the control socket — it is the process that may — and the child serves on
    /// the descriptor it inherited, so the confinement needs no `network-bind` grant. Nothing about
    /// the single-instance claim moves: the launcher's bind is still what a second server meets, and
    /// the plane records the path that bind created, so `state.control_name` names the socket the
    /// child actually answers on.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_adopted_socket_serves_on_the_channel_the_launcher_bound() {
        let dirs = Dirs::new();
        let socket = dirs.socket();
        // The launcher's half: the bind that reclaims the path and makes the single-instance claim.
        let channel = bind_unix(&socket).expect("the launcher binds the control channel");
        // The child's half: a descriptor for the same socket. This is what crosses the exec in
        // production — the launcher clears `FD_CLOEXEC` on `Channel::socket` and spells the number
        // into argv, and the platform crate rebuilds the listener from it.
        let inherited = StdUnixListener::from(
            channel
                .socket()
                .try_clone_to_owned()
                .expect("a descriptor for the inherited socket"),
        );

        let local = LocalForm::Unix(socket.clone());
        let running = Running::start_adopted(&dirs, local_selection(&local), inherited).await;
        assert_eq!(
            running.report.state.as_ref().expect("state").control_name,
            socket.to_string_lossy(),
            "the adopted channel records the path the launcher bound"
        );

        // The claim is still the launcher's: its bind is what a second server meets, and the probe
        // refuses rather than unlinking a live socket.
        assert!(
            bind_unix(&socket).is_err(),
            "another server is already listening on the adopted channel"
        );

        let token = running.token();
        let answer = local_request(&local, "/status", Method::GET, Some(&token)).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
        assert!(answer.body.contains("\"running\":true"), "{}", answer.body);

        running.stop(&dirs).await;
        // The plane removes the socket file it recorded, exactly as the binding half does, and the
        // launcher's own copy is the last owner of the object this leg drops.
        assert!(!socket.exists(), "`shutdown` removes the adopted socket");
        drop(channel);
    }

    /// **The Windows channel's one claim, checked rather than trusted**: a second instance cannot
    /// bind the same name.
    ///
    /// **The DACL half this leg used to carry is gone with what it asserted.** The channel installs
    /// no descriptor any more — [`pipe::create_pipe`] hands `CreateNamedPipeW` a null
    /// `SECURITY_ATTRIBUTES`, so each instance takes Windows' own default named-pipe descriptor —
    /// and the read-back that answered "the current user and nobody else", on the pipe and on the state
    /// file alike, had no subject left. What holds the channel is `PIPE_REJECT_REMOTE_CLIENTS` and
    /// the access token that guards `/status` and `/stop` (`Auth::Local`), which the story legs
    /// assert over a real round trip.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_pipe_refuses_a_second_instance() {
        let dirs = Dirs::new();
        let local = dirs.local();
        let running = Running::start(&dirs, local_selection(&local), None).await;

        // The single-instance claim, as the CLI's second start meets it: the same name, refused.
        let (stop, _rx) = watch::channel(false);
        let refused = Plane::bind(Options {
            selection: local_selection(&local),
            state_dir: dirs.dir().to_path_buf(),
            root: dirs.root().to_path_buf(),
            host: "127.0.0.1".to_string(),
            stop,
            gauges: None,
            token: None,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        });
        let err = refused.expect_err("a second instance is refused");
        assert!(
            format!("{err}").contains("already"),
            "names the reason: {err}"
        );

        // Ended through the channel rather than by a second `client::stop` dial: the stop path over
        // it is the story leg's and the `202` leg's, both of which reach it after a full round trip.
        running.end(&dirs).await;
    }

    /// A handoff with fewer instances than the channel needs is **refused, not served**: one
    /// instance answers its first client and is then connected for as long as it serves, and a
    /// confined child cannot create the next one — so the name would go deaf, which is the defect
    /// the pool removes. The refusal happens before any handle is converted, which is why the
    /// value below never has to name a real object.
    #[cfg(windows)]
    #[test]
    fn adopting_fewer_instances_than_the_pool_needs_is_refused() {
        let (stop, _rx) = watch::channel(false);
        let options = || Options {
            selection: Selection {
                local: Some(LocalForm::Pipe(r"\\.\pipe\tinio-adopt".to_string())),
                ..Selection::default()
            },
            state_dir: PathBuf::from("/run/tinio"),
            root: PathBuf::from("/srv/data"),
            host: "127.0.0.1".to_string(),
            stop: stop.clone(),
            gauges: None,
            token: None,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        };

        for short in [
            Vec::new(),
            vec![0x1000usize as RawHandle; bind::INSTANCES - 1],
        ] {
            let rendered = Plane::adopt(options(), &short)
                .expect_err("a handoff shorter than the pool is refused")
                .to_string();
            assert!(
                rendered.contains("whole pool"),
                "names the refusal: {rendered}"
            );
            assert!(
                rendered.contains("deaf"),
                "names what a single instance costs: {rendered}"
            );
            assert!(
                rendered.contains(&bind::INSTANCES.to_string()),
                "names the count the channel needs: {rendered}"
            );
        }
    }
}
